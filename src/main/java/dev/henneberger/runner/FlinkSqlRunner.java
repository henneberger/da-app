package dev.henneberger.runner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.ExplainDetail;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FlinkSqlRunner {
  private FlinkSqlRunner() {}

  private static final Logger LOGGER = LoggerFactory.getLogger(FlinkSqlRunner.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Pattern TABLE_LIST_PATTERN =
      Pattern.compile("table=\\[\\[([^\\]]+)\\]\\]|table=\\[([^\\]]+)\\]");
  private static final Pattern PLAN_JSON_START =
      Pattern.compile("==\\s*Physical Execution Plan\\s*==");
  private static final Pattern WITH_BLOCK_PATTERN =
      Pattern.compile("WITH\\s*\\((?s)(.*)\\)");
  private static final Pattern WITH_ENTRY_PATTERN =
      Pattern.compile("'([^']+)'\\s*=\\s*'([^']*)'");
  private static final Configuration GLOBAL_FLINK_CONFIG = GlobalConfiguration.loadConfiguration();

  public static void main(String[] args) throws Exception {
    LOGGER.info("Starting FlinkSqlRunner with args={}", java.util.Arrays.toString(args));
    String sqlPath = parseSqlPath(args);
    LOGGER.info("Loading SQL from path={}", sqlPath);
    String sql = Files.readString(Path.of(sqlPath), StandardCharsets.UTF_8);
    LOGGER.info("Loaded SQL bytes={}", sql.length());

    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
    TableEnvironment tableEnvironment = TableEnvironment.create(settings);
    LOGGER.info("TableEnvironment created in streaming mode");

    runSql(tableEnvironment, sql);
  }

  private static String parseSqlPath(String[] args) {
    LOGGER.info("Parsing --sql argument");
    for (int i = 0; i < args.length - 1; i++) {
      if ("--sql".equals(args[i])) {
        String path = args[i + 1];
        LOGGER.info("Found --sql path={}", path);
        return path;
      }
    }
    throw new IllegalArgumentException("Missing --sql /path/to/job.sql argument");
  }

  public static void runSql(TableEnvironment tableEnvironment, String sql) {
    LOGGER.info("Parsing SQL into statements");
    List<String> statements = SqlUtils.parseStatements(sql);
    LOGGER.info("Parsed statements count={}", statements.size());
    List<String> dmlStatements = new ArrayList<>();
    for (String statement : statements) {
      String trimmed = statement == null ? "" : statement.trim();
      String upper = trimmed.toUpperCase(Locale.ROOT);
      boolean isInsert = upper.startsWith("INSERT")
          || upper.startsWith("EXECUTE STATEMENT SET")
          || upper.startsWith("BEGIN STATEMENT SET");
      LOGGER.info("Statement classified isInsert={} trimmedPreview={}",
          isInsert, trimmed.length() > 120 ? trimmed.substring(0, 120) + "..." : trimmed);
      if (isInsert) {
        dmlStatements.add(statement);
        continue;
      }
      if (!trimmed.isEmpty()) {
        LOGGER.info("Executing non-DML statement");
        tableEnvironment.executeSql(statement);
      }
    }

    LOGGER.info("DML statements count={}", dmlStatements.size());
    emitOpenLineageIfConfigured(tableEnvironment, dmlStatements);

    for (String statement : dmlStatements) {
      LOGGER.info("Executing DML statement");
      TableResult result = tableEnvironment.executeSql(statement);
      Optional.ofNullable(result.getJobClient().orElse(null))
          .ifPresent(
              jobClient -> {
                try {
                  LOGGER.info("Waiting for job execution result");
                  jobClient.getJobExecutionResult().get();
                  LOGGER.info("Job execution completed");
                } catch (Exception e) {
                  throw new RuntimeException("Flink SQL job failed", e);
                }
              });
    }
  }

  static void emitOpenLineageIfConfigured(TableEnvironment tableEnvironment, List<String> statements) {
    String transportType = resolveOpenLineageTransportType(tableEnvironment);
    LOGGER.info("OpenLineage transportType={}", transportType);
    if (transportType == null || transportType.isBlank()) {
      LOGGER.info("OpenLineage transport not configured, skipping emit");
      return;
    }
    String namespace = resolveOpenLineageNamespace(tableEnvironment);
    String jobName = resolveJobName(tableEnvironment);
    LOGGER.info("OpenLineage namespace={} jobName={}", namespace, jobName);
    Set<String> inputs = new LinkedHashSet<>();
    Set<String> outputs = new LinkedHashSet<>();
    for (String statement : statements) {
      try {
        LOGGER.info("Explaining statement for lineage");
        String explain = tableEnvironment.explainSql(statement, ExplainDetail.JSON_EXECUTION_PLAN);
        extractLineageFromExplain(explain, inputs, outputs);
        LOGGER.info("Explain lineage extracted inputs={} outputs={}", inputs.size(), outputs.size());
      } catch (Exception ignored) {
        // Skip statements that cannot be explained.
        LOGGER.warn("Explain failed for statement, skipping lineage extraction");
      }
    }
    if (inputs.isEmpty() && outputs.isEmpty()) {
      LOGGER.info("No inputs or outputs detected, skipping OpenLineage emit");
      return;
    }
    try {
      LOGGER.info("Sending OpenLineage event inputs={} outputs={}", inputs, outputs);
      sendOpenLineageEvent(tableEnvironment, transportType, namespace, jobName, inputs, outputs);
      LOGGER.info("OpenLineage event emitted transport={} inputs={} outputs={}",
          transportType, inputs.size(), outputs.size());
    } catch (Exception exc) {
      throw new RuntimeException("Failed to emit OpenLineage payload", exc);
    }
  }

  private static String resolveOpenLineageUrl(TableEnvironment tableEnvironment) {
    String env = System.getenv("OPENLINEAGE_URL");
    if (env != null && !env.isBlank()) {
      LOGGER.info("OpenLineage URL from env OPENLINEAGE_URL={}", env);
      return env;
    }
    String url = resolveFromFlinkConfig(tableEnvironment, "openlineage.transport.url", "");
    LOGGER.info("OpenLineage URL from config openlineage.transport.url={}", url);
    return url;
  }

  private static String resolveOpenLineageTransportType(TableEnvironment tableEnvironment) {
    String env = System.getenv("OPENLINEAGE_TRANSPORT_TYPE");
    if (env != null && !env.isBlank()) {
      LOGGER.info("OpenLineage transport type from env={}", env);
      return env;
    }
    String type = resolveFromFlinkConfig(tableEnvironment, "openlineage.transport.type", "");
    LOGGER.info("OpenLineage transport type from config={}", type);
    return type == null ? "" : type.trim();
  }

  private static String resolveOpenLineageKafkaTopic(TableEnvironment tableEnvironment) {
    String env = System.getenv("OPENLINEAGE_KAFKA_TOPIC");
    if (env != null && !env.isBlank()) {
      LOGGER.info("OpenLineage Kafka topic from env={}", env);
      return env;
    }
    String topic = resolveFromFlinkConfig(tableEnvironment, "openlineage.transport.kafka.topic", "");
    LOGGER.info("OpenLineage Kafka topic from config={}", topic);
    return topic;
  }

  private static String resolveOpenLineageKafkaBootstrap(TableEnvironment tableEnvironment) {
    String env = System.getenv("OPENLINEAGE_KAFKA_BOOTSTRAP");
    if (env != null && !env.isBlank()) {
      LOGGER.info("OpenLineage Kafka bootstrap from env={}", env);
      return env;
    }
    String bootstrap = resolveFromFlinkConfig(
        tableEnvironment, "openlineage.transport.kafka.bootstrap.servers", "");
    LOGGER.info("OpenLineage Kafka bootstrap from config={}", bootstrap);
    return bootstrap;
  }

  private static String resolveOpenLineageNamespace(TableEnvironment tableEnvironment) {
    String env = System.getenv("OPENLINEAGE_NAMESPACE");
    if (env != null && !env.isBlank()) {
      LOGGER.info("OpenLineage namespace from env={}", env);
      return env;
    }
    String namespace = resolveFromFlinkConfig(tableEnvironment, "openlineage.namespace", "default");
    LOGGER.info("OpenLineage namespace from config={}", namespace);
    return namespace;
  }

  private static String resolveJobName(TableEnvironment tableEnvironment) {
    String env = System.getenv("FLINK_JOB_NAME");
    if (env != null && !env.isBlank()) {
      LOGGER.info("Job name from env FLINK_JOB_NAME={}", env);
      return env;
    }
    String pipelineName = resolveFromFlinkConfig(tableEnvironment, "pipeline.name", "flink-sql-job");
    LOGGER.info("Job name from config pipeline.name={}", pipelineName);
    return pipelineName == null || pipelineName.isBlank() ? "flink-sql-job" : pipelineName;
  }

  private static String resolveFromFlinkConfig(TableEnvironment tableEnvironment,
                                               String key,
                                               String defaultValue) {
    String tableConfig = tableEnvironment.getConfig().getConfiguration().getString(key, "");
    if (tableConfig != null && !tableConfig.isBlank()) {
      LOGGER.info("Resolved config from TableEnvironment key={} value={}", key, tableConfig);
      return tableConfig;
    }
    String globalValue = GLOBAL_FLINK_CONFIG.getString(key, "");
    if (globalValue != null && !globalValue.isBlank()) {
      LOGGER.info("Resolved config from GlobalConfiguration key={} value={}", key, globalValue);
      return globalValue;
    }
    LOGGER.info("Config key={} not set, using default={}", key, defaultValue);
    return defaultValue;
  }

  private static void extractLineageFromExplain(String explain,
                                                Set<String> inputs,
                                                Set<String> outputs) throws Exception {
    if (explain == null || explain.isBlank()) {
      LOGGER.info("Explain output empty, skipping");
      return;
    }
    String jsonBlock = extractPlanJson(explain);
    if (jsonBlock != null && !jsonBlock.isBlank()) {
      LOGGER.info("Found JSON execution plan block size={}", jsonBlock.length());
      JsonNode root = OBJECT_MAPPER.readTree(jsonBlock);
      walkPlan(root, inputs, outputs);
    } else {
      LOGGER.info("No JSON execution plan block found");
    }
    extractTablesFromText(explain, inputs, outputs);
  }

  private static String extractPlanJson(String explain) {
    Matcher matcher = PLAN_JSON_START.matcher(explain);
    if (!matcher.find()) {
      LOGGER.debug("Physical Execution Plan header not found");
      return null;
    }
    int start = matcher.end();
    int braceIndex = explain.indexOf('{', start);
    if (braceIndex < 0) {
      LOGGER.debug("JSON plan brace not found");
      return null;
    }
    return explain.substring(braceIndex).trim();
  }

  private static void extractTablesFromText(String explain,
                                            Set<String> inputs,
                                            Set<String> outputs) {
    for (String line : explain.split("\\R")) {
      if (line.contains("TableSourceScan")) {
        inputs.addAll(extractTablesFromDescription(line));
      } else if (line.contains("Sink(") || line.contains("LogicalSink")) {
        outputs.addAll(extractTablesFromDescription(line));
      }
    }
    LOGGER.info("Tables extracted from text inputs={} outputs={}", inputs.size(), outputs.size());
  }

  private static void walkPlan(JsonNode node,
                               Set<String> inputs,
                               Set<String> outputs) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      String type = textValue(node, "type");
      String description = textValue(node, "description");
      boolean isSource = containsIgnoreCase(type, "source")
          || containsIgnoreCase(type, "scan")
          || containsIgnoreCase(description, "TableSourceScan");
      boolean isSink = containsIgnoreCase(type, "sink")
          || containsIgnoreCase(description, "Sink(");
      LOGGER.debug("Plan node type={} isSource={} isSink={}", type, isSource, isSink);

      Set<String> tablesHere = new LinkedHashSet<>();
      tablesHere.addAll(extractTablesFromNode(node));
      tablesHere.addAll(extractTablesFromDescription(description));
      if (node.has("contents")) {
        tablesHere.addAll(extractTablesFromDescription(node.get("contents").asText()));
      }

      if (isSource) {
        inputs.addAll(tablesHere);
      }
      if (isSink) {
        outputs.addAll(tablesHere);
      }

      node.fields().forEachRemaining(entry -> walkPlan(entry.getValue(), inputs, outputs));
      return;
    }
    if (node.isArray()) {
      for (JsonNode child : node) {
        walkPlan(child, inputs, outputs);
      }
    }
  }

  private static Set<String> extractTablesFromNode(JsonNode node) {
    Set<String> tables = new LinkedHashSet<>();
    if (node.has("table")) {
      LOGGER.debug("Found table node");
      tables.addAll(parseTableNode(node.get("table")));
    }
    if (node.has("tableIdentifier")) {
      LOGGER.debug("Found tableIdentifier node");
      tables.addAll(parseTableNode(node.get("tableIdentifier")));
    }
    if (node.has("identifier")) {
      LOGGER.debug("Found identifier node");
      tables.addAll(parseTableNode(node.get("identifier")));
    }
    if (node.has("properties") && node.get("properties").has("table")) {
      LOGGER.debug("Found properties.table node");
      tables.addAll(parseTableNode(node.get("properties").get("table")));
    }
    if (node.has("catalog") && node.has("database")) {
      String table = textValue(node, "table");
      if (table == null || table.isBlank()) {
        table = textValue(node, "name");
      }
      if (table != null && !table.isBlank()) {
        tables.add(node.get("catalog").asText() + "." + node.get("database").asText() + "." + table);
      }
    }
    return tables;
  }

  private static Set<String> extractTablesFromDescription(String description) {
    Set<String> tables = new LinkedHashSet<>();
    if (description == null || description.isBlank()) {
      return tables;
    }
    Matcher matcher = TABLE_LIST_PATTERN.matcher(description);
    while (matcher.find()) {
      String inner = matcher.group(1) != null ? matcher.group(1) : matcher.group(2);
      if (inner == null) {
        continue;
      }
      String parsed = normalizeTableIdentifier(inner);
      if (!parsed.isBlank()) {
        tables.add(parsed);
      }
    }
    return tables;
  }

  private static Set<String> parseTableNode(JsonNode tableNode) {
    Set<String> tables = new LinkedHashSet<>();
    if (tableNode == null || tableNode.isNull()) {
      return tables;
    }
    if (tableNode.isTextual()) {
      String parsed = normalizeTableIdentifier(tableNode.asText());
      if (!parsed.isBlank()) {
        tables.add(parsed);
      }
      return tables;
    }
    if (tableNode.isArray()) {
      if (tableNode.size() == 0) {
        return tables;
      }
      if (tableNode.get(0).isArray()) {
        for (JsonNode item : tableNode) {
          tables.addAll(parseTableNode(item));
        }
        return tables;
      }
      List<String> parts = new ArrayList<>();
      for (JsonNode item : tableNode) {
        parts.add(item.asText());
      }
      String parsed = normalizeTableIdentifier(String.join(".", parts));
      if (!parsed.isBlank()) {
        tables.add(parsed);
      }
    }
    return tables;
  }

  private static String normalizeTableIdentifier(String raw) {
    if (raw == null) {
      return "";
    }
    String trimmed = raw.trim()
        .replace("`", "")
        .replace("\"", "")
        .replace("'", "");
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      trimmed = trimmed.substring(1, trimmed.length() - 1);
    }
    String[] parts = trimmed.split(",");
    if (parts.length > 1) {
      List<String> cleaned = new ArrayList<>();
      for (String part : parts) {
        String value = part.trim();
        if (!value.isEmpty()) {
          cleaned.add(value);
        }
      }
      LOGGER.debug("Normalized table identifier from list raw={} parsed={}", raw, cleaned);
      return String.join(".", cleaned);
    }
    return trimmed;
  }

  private static boolean containsIgnoreCase(String value, String needle) {
    if (value == null || needle == null) {
      return false;
    }
    return value.toLowerCase(Locale.ROOT).contains(needle.toLowerCase(Locale.ROOT));
  }

  private static String textValue(JsonNode node, String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    return value.asText();
  }

  private static void sendOpenLineageEvent(TableEnvironment tableEnvironment,
                                           String transportType,
                                           String namespace,
                                           String jobName,
                                           Set<String> inputs,
                                           Set<String> outputs) throws Exception {
    LOGGER.info("Building OpenLineage payload transport={} namespace={} jobName={}",
        transportType, namespace, jobName);
    ArrayNode inputNodes = OBJECT_MAPPER.createArrayNode();
    for (String input : inputs) {
      LOGGER.info("Building OpenLineage input dataset={}", input);
      inputNodes.add(datasetNode(tableEnvironment, namespace, input));
    }
    ArrayNode outputNodes = OBJECT_MAPPER.createArrayNode();
    for (String output : outputs) {
      LOGGER.info("Building OpenLineage output dataset={}", output);
      outputNodes.add(datasetNode(tableEnvironment, namespace, output));
    }

    ObjectNode payload = OBJECT_MAPPER.createObjectNode();
    payload.put("eventType", "START");
    payload.put("eventTime", Instant.now().toString());
    payload.set("run",
        OBJECT_MAPPER.createObjectNode().put("runId", UUID.randomUUID().toString()));
    payload.set("job",
        OBJECT_MAPPER.createObjectNode()
            .put("namespace", namespace)
            .put("name", jobName));
    payload.set("inputs", inputNodes);
    payload.set("outputs", outputNodes);

    String json = OBJECT_MAPPER.writeValueAsString(payload);
    LOGGER.info("OpenLineage payload size={} bytes", json.length());
    if ("kafka".equalsIgnoreCase(transportType)) {
      String topic = resolveOpenLineageKafkaTopic(tableEnvironment);
      String bootstrap = resolveOpenLineageKafkaBootstrap(tableEnvironment);
      if (topic == null || topic.isBlank()) {
        throw new IllegalStateException("openlineage.transport.kafka.topic is required");
      }
      if (bootstrap == null || bootstrap.isBlank()) {
        throw new IllegalStateException("openlineage.transport.kafka.bootstrap.servers is required");
      }
      LOGGER.info("Emitting OpenLineage event to Kafka topic={} bootstrap={}", topic, bootstrap);
      Properties props = new Properties();
      props.put("bootstrap.servers", bootstrap);
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
      KafkaProducer<String, String> producer = new KafkaProducer<>(props);
      producer.send(new ProducerRecord<>(topic, json));
      producer.flush();
      producer.close();
      return;
    }
    String url = resolveOpenLineageUrl(tableEnvironment);
    if (url == null || url.isBlank()) {
      throw new IllegalStateException("openlineage.transport.url is required for http transport");
    }
    LOGGER.info("Emitting OpenLineage event to HTTP endpoint={}", url);
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(url))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(json))
        .build();
    HttpResponse<String> response =
        HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
    LOGGER.info("OpenLineage HTTP response status={} bodySize={}",
        response.statusCode(),
        response.body() == null ? 0 : response.body().length());
  }

  private static ObjectNode datasetNode(TableEnvironment tableEnvironment,
                                        String defaultNamespace,
                                        String identifier) {
    String namespace = defaultNamespace;
    String name = identifier;
    if (identifier != null && identifier.contains(".")) {
      String[] parts = identifier.split("\\.");
      if (parts.length >= 2) {
        namespace = String.join(".", java.util.Arrays.copyOf(parts, parts.length - 1));
        name = parts[parts.length - 1];
      }
    }
    ObjectNode facets = buildCatalogFacets(tableEnvironment, identifier);
    String resolvedNamespace = namespace;
    if (facets != null && facets.has("dataSource") && facets.get("dataSource").has("name")) {
      resolvedNamespace = facets.get("dataSource").get("name").asText(namespace);
    }
    LOGGER.info("Resolved dataset identifier={} namespace={} name={}", identifier, resolvedNamespace, name);
    ObjectNode node = OBJECT_MAPPER.createObjectNode();
    node.put("namespace", resolvedNamespace);
    node.put("name", name);
    if (facets != null) {
      LOGGER.info("Adding dataset facets keys={}", facets.fieldNames());
      node.set("facets", facets);
    } else {
      LOGGER.info("No dataset facets found for identifier={}", identifier);
    }
    return node;
  }

  private static ObjectNode buildCatalogFacets(TableEnvironment tableEnvironment, String identifier) {
    if (identifier == null || identifier.isBlank()) {
      LOGGER.info("Skipping facets, identifier empty");
      return null;
    }
    ObjectNode facets = OBJECT_MAPPER.createObjectNode();
    ObjectNode dataSourceFacet = null;
    for (String candidate : resolveIdentifierCandidates(identifier)) {
      LOGGER.info("Resolving facets for candidate identifier={}", candidate);
      if (!facets.has("schema")) {
        ObjectNode schemaFacet = describeTable(tableEnvironment, candidate);
        if (schemaFacet != null) {
          facets.set("schema", schemaFacet);
        } else {
          LOGGER.info("No schema facet found for candidate={}", candidate);
        }
      }
      if (!facets.has("ddl")) {
        ObjectNode ddlFacet = showCreateTable(tableEnvironment, candidate);
        if (ddlFacet != null) {
          facets.set("ddl", ddlFacet);
          if (dataSourceFacet == null && ddlFacet.has("options")) {
            dataSourceFacet = buildDataSourceFacet(ddlFacet.get("options"), candidate);
          }
        } else {
          LOGGER.info("No DDL facet found for candidate={}", candidate);
        }
      }
      if (facets.has("schema") && facets.has("ddl")) {
        break;
      }
    }
    if (dataSourceFacet != null) {
      facets.set("dataSource", dataSourceFacet);
    }
    return facets.size() == 0 ? null : facets;
  }

  private static ObjectNode describeTable(TableEnvironment tableEnvironment, String identifier) {
    try {
      LOGGER.info("DESCRIBE {}", identifier);
      TableResult result = tableEnvironment.executeSql("DESCRIBE " + identifier);
      List<ObjectNode> fields = new ArrayList<>();
      result.collect().forEachRemaining(row -> {
        String name = String.valueOf(row.getField(0));
        String type = String.valueOf(row.getField(1));
        if (name == null || name.isBlank() || name.startsWith("#")) {
          return;
        }
        ObjectNode field = OBJECT_MAPPER.createObjectNode();
        field.put("name", name);
        field.put("type", type);
        fields.add(field);
      });
      if (fields.isEmpty()) {
        LOGGER.info("DESCRIBE {} returned no fields", identifier);
        return null;
      }
      LOGGER.info("DESCRIBE {} returned fields={}", identifier, fields.size());
      ObjectNode schema = OBJECT_MAPPER.createObjectNode();
      schema.set("fields", OBJECT_MAPPER.valueToTree(fields));
      return schema;
    } catch (Exception ignored) {
      LOGGER.warn("DESCRIBE {} failed", identifier);
      return null;
    }
  }

  private static ObjectNode showCreateTable(TableEnvironment tableEnvironment, String identifier) {
    try {
      LOGGER.info("SHOW CREATE TABLE {}", identifier);
      TableResult result = tableEnvironment.executeSql("SHOW CREATE TABLE " + identifier);
      String ddl = result.collect().next().getField(0).toString();
      if (ddl == null || ddl.isBlank()) {
        LOGGER.info("SHOW CREATE TABLE {} returned empty DDL", identifier);
        return null;
      }
      ObjectNode ddlNode = OBJECT_MAPPER.createObjectNode();
      ddlNode.put("ddl", ddl);
      ObjectNode options = parseWithOptions(ddl);
      if (options != null) {
        ddlNode.set("options", options);
      }
      return ddlNode;
    } catch (Exception ignored) {
      LOGGER.warn("SHOW CREATE TABLE {} failed", identifier);
      return null;
    }
  }

  private static ObjectNode parseWithOptions(String ddl) {
    if (ddl == null) {
      return null;
    }
    Matcher matcher = WITH_BLOCK_PATTERN.matcher(ddl);
    if (!matcher.find()) {
      LOGGER.debug("No WITH options block found");
      return null;
    }
    String block = matcher.group(1);
    ObjectNode options = OBJECT_MAPPER.createObjectNode();
    Matcher entry = WITH_ENTRY_PATTERN.matcher(block);
    while (entry.find()) {
      String key = entry.group(1);
      String value = entry.group(2);
      options.put(key, value);
    }
    LOGGER.debug("Parsed WITH options count={}", options.size());
    return options.size() == 0 ? null : options;
  }

  private static ObjectNode buildDataSourceFacet(JsonNode options, String identifier) {
    if (options == null || !options.isObject()) {
      return null;
    }
    String connector = textValue(options, "connector");
    if (connector == null || connector.isBlank()) {
      return null;
    }
    ObjectNode dataSource = OBJECT_MAPPER.createObjectNode();
    String name = null;
    String uri = null;
    switch (connector) {
      case "kafka":
        String bootstrap = textValue(options, "properties.bootstrap.servers");
        String topic = textValue(options, "topic");
        if (bootstrap != null && !bootstrap.isBlank()) {
          name = "kafka://" + bootstrap;
          uri = name;
        }
        if (topic != null && !topic.isBlank()) {
          dataSource.put("topic", topic);
        }
        break;
      case "jdbc":
        String url = textValue(options, "url");
        if (url != null && !url.isBlank()) {
          String trimmed = trimTrailingSlash(url);
          name = trimmed;
          uri = trimmed;
        }
        break;
      case "postgres-cdc":
        String host = textValue(options, "hostname");
        String port = textValue(options, "port");
        String database = textValue(options, "database-name");
        if (host != null && database != null) {
          String portSuffix = port == null || port.isBlank() ? "" : ":" + port;
          name = "postgres://" + host + portSuffix + "/" + database;
          uri = name;
        }
        break;
      default:
        name = connector + ":" + identifier;
        uri = name;
        break;
    }
    if (name == null || name.isBlank()) {
      return null;
    }
    dataSource.put("name", name);
    dataSource.put("uri", uri == null ? name : uri);
    return dataSource;
  }

  private static String trimTrailingSlash(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    return trimmed;
  }

  private static List<String> resolveIdentifierCandidates(String identifier) {
    List<String> candidates = new ArrayList<>();
    candidates.add(identifier);
    if (!identifier.contains(".")) {
      candidates.add("default_catalog.default_database." + identifier);
    }
    LOGGER.info("Resolved identifier candidates for {} -> {}", identifier, candidates);
    return candidates;
  }
}
