package dev.henneberger.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.JsonPatch;
import com.github.fge.jsonpatch.JsonPatchException;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ControllerConfiguration
public class FlinkSqlJobReconciler implements Reconciler<FlinkSqlJob> {
  private static final String DEFAULT_FLINK_VERSION = "v1_19";
  private static final String SQL_MOUNT_PATH = "/opt/flink/sql";
  private static final String SQL_FILE_NAME = "job.sql";
  private static final String SQL_VOLUME_NAME = "sql";
  private static final String PYTHON_MOUNT_PATH = "/opt/flink/py";
  private static final String PYTHON_VOLUME_NAME = "python";
  private static final String PYTHON_SRC_MOUNT_PATH = "/opt/flink/py-src";
  private static final String PYTHON_SRC_VOLUME_NAME = "python-src";
  private static final String PYTHON_VENV_PATH = "/opt/flink/py/venv";
  private static final String PYTHON_REQUIREMENTS = "requirements.txt";
  private static final String RUNNER_JAR_URI = "local:///opt/flink/usrlib/flink-sql-runner.jar";
  private static final String FLINK_SQL_JOB_API_VERSION = "dev.henneberger/v1alpha1";
  private static final String FLINK_SQL_JOB_KIND = "FlinkSqlJob";
  private static final String S3_CREDS_SECRET_NAME = "s3-credentials";
  private static final String JOB_STATE_RUNNING = "running";
  private static final String JOB_STATE_SUSPENDED = "suspended";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String MAX_CPU_MILLICORES_ENV = "MAX_CPU_MILLICORES";
  private static final String MAX_MEMORY_MIB_ENV = "MAX_MEMORY_MIB";
  private static final String DEFAULT_SERVICE_ACCOUNT_ENV = "DEFAULT_SERVICE_ACCOUNT";
  private static final String DEFAULT_CHECKPOINT_DIR_ENV = "DEFAULT_CHECKPOINT_DIR";
  private static final String DEFAULT_SAVEPOINT_DIR_ENV = "DEFAULT_SAVEPOINT_DIR";
  private static final Duration DEFAULT_RESCHEDULE = Duration.ofSeconds(5);
  private static final Duration QUEUE_POLL_INTERVAL = Duration.ofSeconds(5);

  private final KubernetesClient client;
  private final int maxCpuMillicores;
  private final int maxMemoryMiB;
  private final String defaultServiceAccount;
  private final String defaultCheckpointDir;
  private final String defaultSavepointDir;

  public FlinkSqlJobReconciler(KubernetesClient client) {
    this.client = client;
    this.maxCpuMillicores = parseMaxMillicores(System.getenv(MAX_CPU_MILLICORES_ENV));
    this.maxMemoryMiB = parseMaxMemoryMiB(System.getenv(MAX_MEMORY_MIB_ENV));
    this.defaultServiceAccount = parseDefaultServiceAccount(System.getenv(DEFAULT_SERVICE_ACCOUNT_ENV));
    this.defaultCheckpointDir = parseDefaultPath(System.getenv(DEFAULT_CHECKPOINT_DIR_ENV));
    this.defaultSavepointDir = parseDefaultPath(System.getenv(DEFAULT_SAVEPOINT_DIR_ENV));
  }

  @Override
  public UpdateControl<FlinkSqlJob> reconcile(FlinkSqlJob resource, Context context) {
    String namespace = resource.getMetadata().getNamespace();
    String name = resource.getMetadata().getName();
    FlinkSqlJobSpec spec = resource.getSpec();

    if (spec == null || spec.getImage() == null || spec.getSql() == null) {
      return updateStatus(resource, "Error", "spec.image and spec.sql are required", name);
    }

    String ownerApiVersion =
        resource.getApiVersion() == null ? FLINK_SQL_JOB_API_VERSION : resource.getApiVersion();
    String ownerKind = resource.getKind() == null ? FLINK_SQL_JOB_KIND : resource.getKind();
    OwnerReference ownerReference = new OwnerReferenceBuilder()
        .withApiVersion(ownerApiVersion)
        .withKind(ownerKind)
        .withName(name)
        .withUid(resource.getMetadata().getUid())
        .withController(true)
        .withBlockOwnerDeletion(true)
        .build();

    FlinkSqlJobStatus status = resource.getStatus();
    if (status == null) {
      status = new FlinkSqlJobStatus();
    }

    FlinkSqlJobSpec.Schedule schedule = spec.getSchedule();
    Duration maxRunTime;
    Duration rekickInterval;
    try {
      maxRunTime = parseDurationOrNull(schedule == null ? null : schedule.getMaxRunTime(),
          "spec.schedule.maxRunTime");
      rekickInterval = parseDurationOrNull(schedule == null ? null : schedule.getRekickInterval(),
          "spec.schedule.rekickInterval");
    } catch (IllegalArgumentException e) {
      return updateStatus(resource, "Error", e.getMessage(), name);
    }
    boolean continuous = maxRunTime == null;

    Instant now = Instant.now();
    Instant lastKickTime = parseInstantOrNull(status.getLastKickTime());
    Instant activeStartTime = parseInstantOrNull(status.getActiveStartTime());

    GenericKubernetesResource existingDeployment =
        client.genericKubernetesResources("flink.apache.org/v1beta1", "FlinkDeployment")
            .inNamespace(namespace)
            .withName(name)
            .get();
    String deploymentJobState = existingDeployment == null ? null : extractJobState(existingDeployment);
    boolean isRunning = existingDeployment != null && !JOB_STATE_SUSPENDED.equalsIgnoreCase(deploymentJobState);
    String lastSavepointPath = existingDeployment == null ? null : extractLastSavepoint(existingDeployment);
    if (lastSavepointPath != null) {
      status.setLastSavepointPath(lastSavepointPath);
    }

    if (isRunning && activeStartTime == null) {
      Instant inferredStart = lastKickTime == null ? now : lastKickTime;
      activeStartTime = inferredStart;
      status.setActiveStartTime(inferredStart.toString());
      if (lastKickTime == null) {
        lastKickTime = inferredStart;
        status.setLastKickTime(inferredStart.toString());
      }
    }

    long generation = resource.getMetadata() != null && resource.getMetadata().getGeneration() != null
        ? resource.getMetadata().getGeneration()
        : 1L;

    if (isRunning && maxRunTime != null && activeStartTime != null
        && now.isAfter(activeStartTime.plus(maxRunTime))) {
      long savepointNonce = now.getEpochSecond();
      try {
        applyFlinkDeployment(
            namespace,
            name,
            spec,
            ownerReference,
            generation,
            JOB_STATE_SUSPENDED,
            savepointNonce,
            null,
            "savepoint");
      } catch (IllegalArgumentException e) {
        return updateStatus(resource, "Error", "Invalid overrides: " + e.getMessage(), name);
      }
      status.setState("Suspended");
      status.setMessage("Max run time reached; savepoint requested and job suspended");
      status.setLastStopTime(now.toString());
      status.setActiveStartTime(null);
      resource.setStatus(status);
      return UpdateControl.updateStatus(resource).rescheduleAfter(
          computeRescheduleAfter(now, lastKickTime, maxRunTime, rekickInterval, continuous));
    }

    if (isRunning) {
      try {
        applyFlinkDeployment(
            namespace,
            name,
            spec,
            ownerReference,
            generation,
            JOB_STATE_RUNNING,
            null,
            status.getLastSavepointPath(),
            null);
      } catch (IllegalArgumentException e) {
        return updateStatus(resource, "Error", "Invalid overrides: " + e.getMessage(), name);
      }
      status.setState("Running");
      status.setMessage("FlinkDeployment running");
      status.setFlinkDeploymentName(name);
      resource.setStatus(status);
      return UpdateControl.updateStatus(resource).rescheduleAfter(
          computeRescheduleAfter(now, lastKickTime, maxRunTime, rekickInterval, continuous));
    }

    boolean dueToStart =
        shouldStart(now, lastKickTime, maxRunTime, rekickInterval, continuous);
    if (dueToStart) {
      QueueDecision queueDecision = evaluateQueue(namespace, now, resource);
      if (!queueDecision.canStart) {
        status.setState("Queued");
        status.setMessage(queueDecision.message);
        status.setFlinkDeploymentName(null);
        resource.setStatus(status);
        Duration rescheduleAfter = computeRescheduleAfter(
            now, lastKickTime, maxRunTime, rekickInterval, continuous);
        if (rescheduleAfter.compareTo(QUEUE_POLL_INTERVAL) > 0) {
          rescheduleAfter = QUEUE_POLL_INTERVAL;
        }
        return UpdateControl.updateStatus(resource).rescheduleAfter(rescheduleAfter);
      }

      try {
        applyFlinkDeployment(
            namespace,
            name,
            spec,
            ownerReference,
            generation,
            JOB_STATE_RUNNING,
            null,
            status.getLastSavepointPath(),
            null);
      } catch (IllegalArgumentException e) {
        return updateStatus(resource, "Error", "Invalid overrides: " + e.getMessage(), name);
      }
      status.setState("Running");
      status.setMessage("FlinkDeployment created or updated");
      status.setFlinkDeploymentName(name);
      status.setLastKickTime(now.toString());
      status.setActiveStartTime(now.toString());
      resource.setStatus(status);
      return UpdateControl.updateStatus(resource).rescheduleAfter(
          computeRescheduleAfter(now, parseInstantOrNull(status.getLastKickTime()),
              maxRunTime, rekickInterval, continuous));
    }

    status.setState("Idle");
    status.setMessage("FlinkDeployment not running");
    status.setFlinkDeploymentName(null);
    resource.setStatus(status);
    return UpdateControl.updateStatus(resource).rescheduleAfter(
        computeRescheduleAfter(now, lastKickTime, maxRunTime, rekickInterval, continuous));
  }

  private void applyFlinkDeployment(
      String namespace,
      String name,
      FlinkSqlJobSpec spec,
      OwnerReference ownerReference,
      long generation,
      String desiredJobState,
      Long savepointTriggerNonce,
      String savepointRestorePath,
      String upgradeModeOverride) {
    String configMapName = name + "-sql";
    String resolvedJobName = spec.getJobName() == null ? name : spec.getJobName();
    PythonArtifacts pythonArtifacts = loadPythonArtifacts(namespace, spec.getPythonRefs());
    String catalogsSql = loadCatalogSql(namespace, spec.getCatalogRefs());
    Map<String, String> configData = new LinkedHashMap<>();
    configData.put(SQL_FILE_NAME, mergeCatalogSql(catalogsSql, spec.getSql()));
    configData.putAll(pythonArtifacts.files);
    ConfigMap configMap = new ConfigMapBuilder()
        .withMetadata(new ObjectMetaBuilder()
            .withName(configMapName)
            .withNamespace(namespace)
            .addToLabels("app", name)
            .withOwnerReferences(ownerReference)
            .build())
        .withData(configData)
        .build();
    client.configMaps().inNamespace(namespace).resource(configMap).createOrReplace();

    Map<String, Object> flinkDeploymentSpec =
        buildFlinkDeploymentSpec(name, resolvedJobName, spec, configMapName, generation, pythonArtifacts,
            desiredJobState, savepointTriggerNonce, savepointRestorePath, upgradeModeOverride);
    flinkDeploymentSpec = applyOverrides(spec, flinkDeploymentSpec);
    GenericKubernetesResource flinkDeployment = new GenericKubernetesResource();
    flinkDeployment.setApiVersion("flink.apache.org/v1beta1");
    flinkDeployment.setKind("FlinkDeployment");
    flinkDeployment.setMetadata(new ObjectMetaBuilder()
        .withName(name)
        .withNamespace(namespace)
        .addToLabels("app", name)
        .withOwnerReferences(ownerReference)
        .build());
    flinkDeployment.setAdditionalProperty("spec", flinkDeploymentSpec);

    client.genericKubernetesResources("flink.apache.org/v1beta1", "FlinkDeployment")
        .inNamespace(namespace)
        .resource(flinkDeployment)
        .createOrReplace();
  }
  private Map<String, Object> buildFlinkDeploymentSpec(
      String name,
      String resolvedJobName,
      FlinkSqlJobSpec spec,
      String configMapName,
      long generation,
      PythonArtifacts pythonArtifacts,
      String desiredJobState,
      Long savepointTriggerNonce,
      String savepointRestorePath,
      String upgradeModeOverride) {
    Map<String, Object> podTemplate =
        buildPodTemplate(configMapName, spec.getImage(), pythonArtifacts);

    Map<String, Object> jobManager = new LinkedHashMap<>();
    jobManager.put("replicas", 1);
    jobManager.put("resource", Map.of("memory", "2048m", "cpu", 1));
    jobManager.put("podTemplate", podTemplate);

    Map<String, Object> taskManager = new LinkedHashMap<>();
    taskManager.put("replicas", 1);
    taskManager.put("resource", Map.of("memory", "2048m", "cpu", 1));
    taskManager.put("podTemplate", podTemplate);

    Map<String, Object> job = new LinkedHashMap<>();
    job.put("name", resolvedJobName);
    job.put("jarURI", RUNNER_JAR_URI);
    job.put("entryClass", "dev.henneberger.runner.FlinkSqlRunner");
    job.put("args", List.of("--sql", SQL_MOUNT_PATH + "/" + SQL_FILE_NAME));
    job.put("parallelism", spec.getParallelism() == null ? 1 : spec.getParallelism());
    String upgradeMode = upgradeModeOverride == null ? "stateless" : upgradeModeOverride;
    job.put("upgradeMode", upgradeMode);
    if (desiredJobState != null) {
      job.put("state", desiredJobState);
    }
    if (savepointTriggerNonce != null) {
      job.put("savepointTriggerNonce", savepointTriggerNonce);
    }
    if (savepointRestorePath != null) {
      job.put("initialSavepointPath", savepointRestorePath);
      job.put("allowNonRestoredState", true);
    }
    // Bump restartNonce on CR updates to force Flink Operator to restart the job.
    job.put("restartNonce", generation);

    Map<String, Object> flinkDeploymentSpec = new LinkedHashMap<>();
    flinkDeploymentSpec.put("image", spec.getImage());
    flinkDeploymentSpec.put("imagePullPolicy", "IfNotPresent");
    flinkDeploymentSpec.put(
        "flinkVersion",
        spec.getFlinkVersion() == null ? DEFAULT_FLINK_VERSION : spec.getFlinkVersion());
    flinkDeploymentSpec.put("job", job);
    flinkDeploymentSpec.put("jobManager", jobManager);
    flinkDeploymentSpec.put("taskManager", taskManager);
    Map<String, Object> flinkConfig = new LinkedHashMap<>();
    if (!pythonArtifacts.files.isEmpty()) {
      flinkConfig.put("python.files", String.join(",", pythonArtifacts.pythonFiles));
      if (pythonArtifacts.requirementsPath != null) {
        flinkConfig.put("python.requirements", pythonArtifacts.requirementsPath);
      }
      String pythonExecutable = PYTHON_VENV_PATH + "/bin/python";
      flinkConfig.put("python.executable", pythonExecutable);
      flinkConfig.put("python.client.executable", pythonExecutable);
    }
    boolean needsSavepointConfig = "savepoint".equalsIgnoreCase(upgradeMode)
        || savepointTriggerNonce != null
        || savepointRestorePath != null;
    if (needsSavepointConfig) {
      ensureSavepointConfig(flinkConfig);
    }
    if (!flinkConfig.isEmpty()) {
      flinkDeploymentSpec.put("flinkConfiguration", flinkConfig);
    }
    String serviceAccount = resolveServiceAccount(spec.getServiceAccount());
    if (serviceAccount != null) {
      flinkDeploymentSpec.put("serviceAccount", serviceAccount);
    }

    return flinkDeploymentSpec;
  }


  private Map<String, Object> applyOverrides(FlinkSqlJobSpec spec, Map<String, Object> baseSpec) {
    FlinkSqlJobSpec.Overrides overrides = spec.getOverrides();
    if (overrides == null) {
      return baseSpec;
    }
    if (overrides.getMerge() != null && !overrides.getMerge().isEmpty()) {
      deepMergeInto(baseSpec, overrides.getMerge());
    }
    if (overrides.getPatches() != null && !overrides.getPatches().isEmpty()) {
      try {
        JsonNode baseNode = OBJECT_MAPPER.valueToTree(baseSpec);
        JsonNode patchNode = OBJECT_MAPPER.valueToTree(overrides.getPatches());
        JsonPatch patch = JsonPatch.fromJson(patchNode);
        JsonNode patched = patch.apply(baseNode);
        return OBJECT_MAPPER.convertValue(patched, new TypeReference<Map<String, Object>>() {});
      } catch (JsonPatchException | IOException e) {
        throw new IllegalArgumentException(e.getMessage(), e);
      }
    }
    return baseSpec;
  }

  private void deepMergeInto(Map<String, Object> target, Map<String, Object> overrides) {
    for (Map.Entry<String, Object> entry : overrides.entrySet()) {
      String key = entry.getKey();
      Object overrideValue = entry.getValue();
      Object existingValue = target.get(key);
      if (existingValue instanceof Map && overrideValue instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> existingMap = (Map<String, Object>) existingValue;
        @SuppressWarnings("unchecked")
        Map<String, Object> overrideMap = (Map<String, Object>) overrideValue;
        // baseSpec is built using Map.of(...) in some places, which creates immutable maps.
        // Convert to a mutable map before merging overrides.
        Map<String, Object> mutableExisting =
            (existingMap instanceof java.util.HashMap || existingMap instanceof java.util.LinkedHashMap)
                ? existingMap
                : new java.util.LinkedHashMap<>(existingMap);
        if (mutableExisting != existingMap) {
          target.put(key, mutableExisting);
        }
        deepMergeInto(mutableExisting, overrideMap);
      } else {
        target.put(key, overrideValue);
      }
    }
  }

  private Map<String, Object> buildPodTemplate(
      String configMapName,
      String image,
      PythonArtifacts pythonArtifacts) {
    List<Map<String, Object>> volumes = new java.util.ArrayList<>();
    List<Map<String, Object>> volumeMounts = new java.util.ArrayList<>();
    List<Map<String, Object>> initContainers = new java.util.ArrayList<>();

    volumes.add(Map.of("name", SQL_VOLUME_NAME, "configMap", Map.of("name", configMapName)));
    volumeMounts.add(Map.of("name", SQL_VOLUME_NAME, "mountPath", SQL_MOUNT_PATH));

    if (!pythonArtifacts.files.isEmpty()) {
      volumes.add(Map.of("name", PYTHON_SRC_VOLUME_NAME, "configMap", Map.of("name", configMapName)));
      volumes.add(Map.of("name", PYTHON_VOLUME_NAME, "emptyDir", Map.of()));
      volumeMounts.add(Map.of("name", PYTHON_VOLUME_NAME, "mountPath", PYTHON_MOUNT_PATH));

      String initCommand =
          "set -e; " +
          "if ls " + PYTHON_SRC_MOUNT_PATH + "/*.py >/dev/null 2>&1; then " +
          "  cp " + PYTHON_SRC_MOUNT_PATH + "/*.py " + PYTHON_MOUNT_PATH + "/; " +
          "fi; " +
          "if [ -f " + PYTHON_SRC_MOUNT_PATH + "/" + PYTHON_REQUIREMENTS + " ]; then " +
          "  cp " + PYTHON_SRC_MOUNT_PATH + "/" + PYTHON_REQUIREMENTS + " " + PYTHON_MOUNT_PATH + "/; " +
          "fi; " +
          "python3 -m venv --system-site-packages " + PYTHON_VENV_PATH + "; " +
          "if [ -f " + PYTHON_MOUNT_PATH + "/" + PYTHON_REQUIREMENTS + " ]; then " +
          "  " + PYTHON_VENV_PATH + "/bin/pip install --no-cache-dir -r " +
          PYTHON_MOUNT_PATH + "/" + PYTHON_REQUIREMENTS + "; " +
          "fi";

      Map<String, Object> initContainer = new LinkedHashMap<>();
      initContainer.put("name", "python-setup");
      initContainer.put("image", image);
      initContainer.put("imagePullPolicy", "IfNotPresent");
      initContainer.put("command", List.of("/bin/sh", "-c", initCommand));
      initContainer.put("volumeMounts", List.of(
          Map.of("name", PYTHON_SRC_VOLUME_NAME, "mountPath", PYTHON_SRC_MOUNT_PATH),
          Map.of("name", PYTHON_VOLUME_NAME, "mountPath", PYTHON_MOUNT_PATH)
      ));
      initContainers.add(initContainer);
    }

    // Some clusters/jobs don't need S3 access (or provide credentials another way).
    // Mark the envFrom secret optional so a missing Secret doesn't prevent the job from starting.
    Map<String, Object> s3EnvFromSecret =
        Map.of("secretRef", Map.of("name", S3_CREDS_SECRET_NAME, "optional", true));
    List<Map<String, Object>> envFrom = List.of(s3EnvFromSecret);

    Map<String, Object> container = Map.of(
        "name", "flink-main-container",
        "volumeMounts", volumeMounts,
        "envFrom", envFrom
    );

    Map<String, Object> podSpec = new LinkedHashMap<>();
    podSpec.put("volumes", volumes);
    podSpec.put("containers", List.of(container));
    if (!initContainers.isEmpty()) {
      podSpec.put("initContainers", initContainers);
    }

    Map<String, Object> podTemplate = new LinkedHashMap<>();
    podTemplate.put("spec", podSpec);
    return podTemplate;
  }

  private PythonArtifacts loadPythonArtifacts(String namespace, List<String> pythonRefs) {
    if (pythonRefs == null || pythonRefs.isEmpty()) {
      return PythonArtifacts.empty();
    }
    Map<String, String> files = new LinkedHashMap<>();
    List<String> pythonFiles = new java.util.ArrayList<>();
    StringBuilder requirements = new StringBuilder();

    for (String ref : pythonRefs) {
      FlinkPythonFunction function =
          client.resources(FlinkPythonFunction.class).inNamespace(namespace).withName(ref).get();
      if (function == null || function.getSpec() == null || function.getSpec().getFunction() == null) {
        throw new IllegalStateException("FlinkPythonFunction " + ref + " not found");
      }
      FlinkPythonFunctionSpec spec = function.getSpec();
      String module = spec.getFunction().getModule() == null ? null : spec.getFunction().getModule().getInline();
      if (module == null || module.isBlank()) {
        throw new IllegalStateException("FlinkPythonFunction " + ref + " missing module.inline");
      }
      String functionName = spec.getFunction().getName();
      if (functionName == null || functionName.isBlank()) {
        throw new IllegalStateException("FlinkPythonFunction " + ref + " missing function.name");
      }
      String moduleName = moduleName(functionName);
      if (moduleName.contains(".")) {
        throw new IllegalStateException(
          "FlinkPythonFunction " + ref + " has nested module '" + moduleName + "'; only single-module names supported");
      }
      String filename = moduleName + ".py";
      files.put(filename, module);
      pythonFiles.add(PYTHON_MOUNT_PATH + "/" + filename);

      if (spec.getRequirements() != null && spec.getRequirements().getInline() != null) {
        if (requirements.length() > 0) {
          requirements.append("\n");
        }
        requirements.append(spec.getRequirements().getInline().trim());
      }
    }

    String requirementsPath = null;
    if (requirements.length() > 0) {
      files.put(PYTHON_REQUIREMENTS, requirements.toString() + "\n");
      requirementsPath = PYTHON_MOUNT_PATH + "/" + PYTHON_REQUIREMENTS;
    }

    pythonFiles = new java.util.ArrayList<>(new java.util.LinkedHashSet<>(pythonFiles));
    return new PythonArtifacts(files, pythonFiles, requirementsPath);
  }

  private String loadCatalogSql(String namespace, List<String> catalogRefs) {
    if (catalogRefs == null || catalogRefs.isEmpty()) {
      return null;
    }
    StringBuilder combined = new StringBuilder();
    for (String ref : catalogRefs) {
      FlinkCatalog catalog =
          client.resources(FlinkCatalog.class).inNamespace(namespace).withName(ref).get();
      if (catalog == null || catalog.getSpec() == null) {
        throw new IllegalStateException("FlinkCatalog " + ref + " not found");
      }
      String sql = catalog.getSpec().getSql();
      if (sql == null || sql.isBlank()) {
        throw new IllegalStateException("FlinkCatalog " + ref + " missing spec.sql");
      }
      if (combined.length() > 0) {
        combined.append("\n\n");
      }
      combined.append(sql.trim());
    }
    return combined.toString();
  }

  private String mergeCatalogSql(String catalogsSql, String jobSql) {
    if (catalogsSql == null || catalogsSql.isBlank()) {
      return jobSql;
    }
    if (jobSql == null || jobSql.isBlank()) {
      return catalogsSql;
    }
    return catalogsSql.trim() + "\n\n" + jobSql.trim();
  }

  private String moduleName(String functionName) {
    int lastDot = functionName.lastIndexOf('.');
    if (lastDot <= 0) {
      return functionName;
    }
    return functionName.substring(0, lastDot);
  }

  private static final class PythonArtifacts {
    private final Map<String, String> files;
    private final List<String> pythonFiles;
    private final String requirementsPath;

    private PythonArtifacts(Map<String, String> files, List<String> pythonFiles, String requirementsPath) {
      this.files = files;
      this.pythonFiles = pythonFiles;
      this.requirementsPath = requirementsPath;
    }

    private static PythonArtifacts empty() {
      return new PythonArtifacts(Map.of(), List.of(), null);
    }
  }

  private UpdateControl<FlinkSqlJob> updateStatus(
      FlinkSqlJob resource, String state, String message, String flinkDeploymentName) {
    FlinkSqlJobStatus status = resource.getStatus();
    if (status == null) {
      status = new FlinkSqlJobStatus();
    }
    status.setState(state);
    status.setMessage(message);
    status.setFlinkDeploymentName(flinkDeploymentName);
    resource.setStatus(status);
    return UpdateControl.updateStatus(resource);
  }

  private String extractJobState(GenericKubernetesResource deployment) {
    Map<String, Object> spec = asMap(deployment.getAdditionalProperties().get("spec"));
    Map<String, Object> job = asMap(spec == null ? null : spec.get("job"));
    Object state = job == null ? null : job.get("state");
    return state == null ? null : state.toString();
  }

  private String extractLastSavepoint(GenericKubernetesResource deployment) {
    Map<String, Object> status = asMap(deployment.getAdditionalProperties().get("status"));
    Map<String, Object> jobStatus = asMap(status == null ? null : status.get("jobStatus"));
    Map<String, Object> savepointInfo = asMap(jobStatus == null ? null : jobStatus.get("savepointInfo"));
    Map<String, Object> lastSavepoint = asMap(savepointInfo == null ? null : savepointInfo.get("lastSavepoint"));
    Object location = lastSavepoint == null ? null : lastSavepoint.get("location");
    return location == null ? null : location.toString();
  }

  private QueueDecision evaluateQueue(String namespace, Instant now, FlinkSqlJob currentJob) {
    if (maxCpuMillicores <= 0 && maxMemoryMiB <= 0) {
      return new QueueDecision(true, "Queue disabled");
    }
    List<FlinkSqlJob> jobs = client.resources(FlinkSqlJob.class).inNamespace(namespace).list().getItems();
    Set<String> running = new HashSet<>();
    ResourceTotals runningTotals = new ResourceTotals();
    for (FlinkSqlJob job : jobs) {
      if (job.getMetadata() == null || job.getMetadata().getName() == null) {
        continue;
      }
      FlinkSqlJobStatus jobStatus = job.getStatus();
      if (jobStatus != null && "Running".equalsIgnoreCase(jobStatus.getState())) {
        String runningName = job.getMetadata().getName();
        running.add(runningName);
        ResourceRequest request = desiredResources(job.getSpec());
        runningTotals.add(request);
      }
    }
    ResourceCapacity capacity = new ResourceCapacity(maxCpuMillicores, maxMemoryMiB);
    if (!capacity.hasCapacityFor(runningTotals)) {
      return new QueueDecision(false, "Queue full (capacity exhausted)");
    }

    List<JobCandidate> candidates = new ArrayList<>();
    for (FlinkSqlJob job : jobs) {
      if (job.getMetadata() == null || job.getMetadata().getName() == null) {
        continue;
      }
      String jobName = job.getMetadata().getName();
      if (running.contains(jobName)) {
        continue;
      }
      FlinkSqlJobSpec jobSpec = job.getSpec();
      if (jobSpec == null) {
        continue;
      }
      FlinkSqlJobSpec.Schedule schedule = jobSpec.getSchedule();
      Duration jobMaxRunTime;
      Duration jobRekickInterval;
      try {
        jobMaxRunTime = parseDurationOrNull(
            schedule == null ? null : schedule.getMaxRunTime(), "spec.schedule.maxRunTime");
        jobRekickInterval = parseDurationOrNull(
            schedule == null ? null : schedule.getRekickInterval(), "spec.schedule.rekickInterval");
      } catch (IllegalArgumentException e) {
        continue;
      }
      boolean jobContinuous = jobMaxRunTime == null;
      FlinkSqlJobStatus jobStatus = job.getStatus();
      Instant jobLastKick = parseInstantOrNull(jobStatus == null ? null : jobStatus.getLastKickTime());
      boolean jobDue = shouldStart(now, jobLastKick, jobMaxRunTime, jobRekickInterval, jobContinuous);
      if (!jobDue) {
        continue;
      }
      int priority = schedule != null && schedule.getPriority() != null ? schedule.getPriority() : 0;
      ResourceRequest request = desiredResources(jobSpec);
      candidates.add(new JobCandidate(jobName, priority, jobLastKick, request));
    }

    candidates.sort(Comparator
        .comparingInt(JobCandidate::priority).reversed()
        .thenComparing(JobCandidate::lastKickTime, Comparator.nullsFirst(Comparator.naturalOrder()))
        .thenComparing(JobCandidate::name));

    ResourceTotals used = new ResourceTotals(runningTotals);
    String currentName = currentJob.getMetadata() == null ? null : currentJob.getMetadata().getName();
    ResourceRequest currentRequest = desiredResources(currentJob.getSpec());
    for (int i = 0; i < candidates.size(); i++) {
      JobCandidate candidate = candidates.get(i);
      if (capacity.canFit(used, candidate.request)) {
        used.add(candidate.request);
        if (candidate.name.equals(currentName)) {
          return new QueueDecision(true, "Resources available");
        }
      } else if (candidate.name.equals(currentName)) {
        return new QueueDecision(false, "Queued (needs cpu "
            + currentRequest.cpuMillicores + "m, mem " + currentRequest.memoryMiB
            + "Mi; capacity cpu " + capacity.cpuMillicores + "m, mem " + capacity.memoryMiB + "Mi)");
      }
    }

    return new QueueDecision(false, "Queued (insufficient resources)");
  }

  private int parseMaxMillicores(String value) {
    if (value == null || value.isBlank()) {
      return 0;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return Math.max(parsed, 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private int parseMaxMemoryMiB(String value) {
    if (value == null || value.isBlank()) {
      return 0;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      return Math.max(parsed, 0);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private String parseDefaultServiceAccount(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private String resolveServiceAccount(String serviceAccount) {
    if (serviceAccount != null && !serviceAccount.isBlank()) {
      return serviceAccount;
    }
    return defaultServiceAccount;
  }

  private boolean shouldStart(
      Instant now,
      Instant lastKickTime,
      Duration maxRunTime,
      Duration rekickInterval,
      boolean continuous) {
    if (continuous) {
      return true;
    }
    if (rekickInterval == null) {
      return lastKickTime == null;
    }
    if (lastKickTime == null) {
      return true;
    }
    return !now.isBefore(lastKickTime.plus(rekickInterval));
  }

  private Duration computeRescheduleAfter(
      Instant now,
      Instant lastKickTime,
      Duration maxRunTime,
      Duration rekickInterval,
      boolean continuous) {
    Duration next = DEFAULT_RESCHEDULE;
    if (maxRunTime != null && lastKickTime != null) {
      Duration untilStop = Duration.between(now, lastKickTime.plus(maxRunTime));
      if (!untilStop.isNegative() && untilStop.compareTo(next) < 0) {
        next = untilStop;
      }
    }
    if (!continuous && rekickInterval != null && lastKickTime != null) {
      Duration untilStart = Duration.between(now, lastKickTime.plus(rekickInterval));
      if (!untilStart.isNegative() && untilStart.compareTo(next) < 0) {
        next = untilStart;
      }
    }
    if (next.isZero() || next.isNegative()) {
      return Duration.ofSeconds(5);
    }
    return next;
  }

  private Instant parseInstantOrNull(String value) {
    if (value == null || value.isBlank()) {
      return null;
    }
    return Instant.parse(value);
  }

  private Duration parseDurationOrNull(String value, String field) {
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      if (value.startsWith("P") || value.startsWith("p")) {
        return Duration.parse(value);
      }
      return parseShortDuration(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(field + " is invalid: " + e.getMessage());
    }
  }

  private Duration parseShortDuration(String value) {
    Pattern pattern = Pattern.compile("(\\d+)([smhd])", Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(value);
    int index = 0;
    Duration total = Duration.ZERO;
    while (matcher.find()) {
      if (matcher.start() != index) {
        throw new IllegalArgumentException("unsupported duration format: " + value);
      }
      long amount = Long.parseLong(matcher.group(1));
      String unit = matcher.group(2).toLowerCase();
      switch (unit) {
        case "s":
          total = total.plusSeconds(amount);
          break;
        case "m":
          total = total.plusMinutes(amount);
          break;
        case "h":
          total = total.plusHours(amount);
          break;
        case "d":
          total = total.plusDays(amount);
          break;
        default:
          throw new IllegalArgumentException("unsupported duration unit: " + unit);
      }
      index = matcher.end();
    }
    if (index != value.length() || total.isZero()) {
      throw new IllegalArgumentException("unsupported duration format: " + value);
    }
    return total;
  }

  private String parseDefaultPath(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private void ensureSavepointConfig(Map<String, Object> flinkConfig) {
    String checkpointDir = asString(flinkConfig.get("execution.checkpointing.dir"));
    String savepointDir = asString(flinkConfig.get("state.savepoints.dir"));
    if (checkpointDir == null && defaultCheckpointDir != null) {
      flinkConfig.put("execution.checkpointing.dir", defaultCheckpointDir);
    }
    if (savepointDir == null && defaultSavepointDir != null) {
      flinkConfig.put("state.savepoints.dir", defaultSavepointDir);
    }
    checkpointDir = asString(flinkConfig.get("execution.checkpointing.dir"));
    savepointDir = asString(flinkConfig.get("state.savepoints.dir"));
    if (checkpointDir == null || savepointDir == null) {
      throw new IllegalArgumentException(
          "Savepoint requested but execution.checkpointing.dir and state.savepoints.dir are not configured");
    }
  }

  private ResourceRequest desiredResources(FlinkSqlJobSpec spec) {
    int jmReplicas = 1;
    int tmReplicas = 1;
    int jmCpu = 1000;
    int tmCpu = 1000;
    int jmMem = 2048;
    int tmMem = 2048;

    FlinkSqlJobSpec.Overrides overrides = spec == null ? null : spec.getOverrides();
    Map<String, Object> merge = overrides == null ? null : overrides.getMerge();
    if (merge != null) {
      Map<String, Object> jm = asMap(merge.get("jobManager"));
      Map<String, Object> tm = asMap(merge.get("taskManager"));
      Integer jmReplicasOverride = asInt(jm == null ? null : jm.get("replicas"));
      Integer tmReplicasOverride = asInt(tm == null ? null : tm.get("replicas"));
      if (jmReplicasOverride != null) {
        jmReplicas = jmReplicasOverride;
      }
      if (tmReplicasOverride != null) {
        tmReplicas = tmReplicasOverride;
      }
      Map<String, Object> jmResource = asMap(jm == null ? null : jm.get("resource"));
      Map<String, Object> tmResource = asMap(tm == null ? null : tm.get("resource"));
      Integer jmCpuOverride = parseCpuMillicores(jmResource == null ? null : jmResource.get("cpu"));
      Integer tmCpuOverride = parseCpuMillicores(tmResource == null ? null : tmResource.get("cpu"));
      Integer jmMemOverride = parseMemoryMiB(jmResource == null ? null : jmResource.get("memory"));
      Integer tmMemOverride = parseMemoryMiB(tmResource == null ? null : tmResource.get("memory"));
      if (jmCpuOverride != null) {
        jmCpu = jmCpuOverride;
      }
      if (tmCpuOverride != null) {
        tmCpu = tmCpuOverride;
      }
      if (jmMemOverride != null) {
        jmMem = jmMemOverride;
      }
      if (tmMemOverride != null) {
        tmMem = tmMemOverride;
      }
    }

    return new ResourceRequest(jmReplicas, tmReplicas, jmCpu, tmCpu, jmMem, tmMem);
  }

  private Integer parseCpuMillicores(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return (int) (Double.parseDouble(value.toString()) * 1000);
    }
    String text = value.toString().trim().toLowerCase();
    if (text.endsWith("m")) {
      return Integer.parseInt(text.substring(0, text.length() - 1));
    }
    return (int) (Double.parseDouble(text) * 1000);
  }

  private Integer parseMemoryMiB(Object value) {
    if (value == null) {
      return null;
    }
    String text = value.toString().trim().toLowerCase();
    if (text.endsWith("mi")) {
      return Integer.parseInt(text.substring(0, text.length() - 2));
    }
    if (text.endsWith("m")) {
      return Integer.parseInt(text.substring(0, text.length() - 1));
    }
    if (text.endsWith("gi")) {
      return Integer.parseInt(text.substring(0, text.length() - 2)) * 1024;
    }
    if (text.endsWith("g")) {
      return Integer.parseInt(text.substring(0, text.length() - 1)) * 1024;
    }
    if (text.endsWith("ki")) {
      return Integer.parseInt(text.substring(0, text.length() - 2)) / 1024;
    }
    return Integer.parseInt(text);
  }

  private Map<String, Object> asMap(Object value) {
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> map = (Map<String, Object>) value;
      return map;
    }
    return null;
  }

  private Integer asInt(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    try {
      return Integer.parseInt(value.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private String asString(Object value) {
    if (value == null) {
      return null;
    }
    String text = value.toString().trim();
    return text.isEmpty() ? null : text;
  }

  private static final class QueueDecision {
    private final boolean canStart;
    private final String message;

    private QueueDecision(boolean canStart, String message) {
      this.canStart = canStart;
      this.message = message;
    }
  }

  private static final class JobCandidate {
    private final String name;
    private final int priority;
    private final Instant lastKickTime;
    private final ResourceRequest request;

    private JobCandidate(String name, int priority, Instant lastKickTime, ResourceRequest request) {
      this.name = name;
      this.priority = priority;
      this.lastKickTime = lastKickTime;
      this.request = request;
    }

    private String name() {
      return name;
    }

    private int priority() {
      return priority;
    }

    private Instant lastKickTime() {
      return lastKickTime;
    }
  }

  private static final class ResourceCapacity {
    private final int cpuMillicores;
    private final int memoryMiB;

    private ResourceCapacity(int cpuMillicores, int memoryMiB) {
      this.cpuMillicores = cpuMillicores;
      this.memoryMiB = memoryMiB;
    }

    private boolean hasCapacityFor(ResourceTotals totals) {
      if (cpuMillicores > 0 && totals.cpuMillicores > cpuMillicores) {
        return false;
      }
      if (memoryMiB > 0 && totals.memoryMiB > memoryMiB) {
        return false;
      }
      return true;
    }

    private boolean canFit(ResourceTotals used, ResourceRequest request) {
      if (cpuMillicores > 0 && used.cpuMillicores + request.cpuMillicores > cpuMillicores) {
        return false;
      }
      if (memoryMiB > 0 && used.memoryMiB + request.memoryMiB > memoryMiB) {
        return false;
      }
      return true;
    }
  }

  private static final class ResourceTotals {
    private int cpuMillicores;
    private int memoryMiB;

    private ResourceTotals() {}

    private ResourceTotals(ResourceTotals other) {
      this.cpuMillicores = other.cpuMillicores;
      this.memoryMiB = other.memoryMiB;
    }

    private void add(ResourceRequest request) {
      this.cpuMillicores += request.cpuMillicores;
      this.memoryMiB += request.memoryMiB;
    }
  }

  private static final class ResourceRequest {
    private final int cpuMillicores;
    private final int memoryMiB;

    private ResourceRequest(int jmReplicas, int tmReplicas, int jmCpu, int tmCpu, int jmMem, int tmMem) {
      this.cpuMillicores = jmReplicas * jmCpu + tmReplicas * tmCpu;
      this.memoryMiB = jmReplicas * jmMem + tmReplicas * tmMem;
    }
  }
}
