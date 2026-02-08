package dev.henneberger.operator.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.henneberger.operator.crd.Connection;
import dev.henneberger.operator.crd.ConnectionSpec;
import dev.henneberger.operator.crd.DuckDBConnection;
import dev.henneberger.operator.crd.DuckDBConnectionSpec;
import dev.henneberger.operator.crd.DynamoDbConnection;
import dev.henneberger.operator.crd.DynamoDbConnectionSpec;
import dev.henneberger.operator.crd.ElasticsearchConnection;
import dev.henneberger.operator.crd.ElasticsearchConnectionSpec;
import dev.henneberger.operator.crd.GraphQLSchema;
import dev.henneberger.operator.crd.GraphQLSchemaSpec;
import dev.henneberger.operator.crd.HealthCheckSpec;
import dev.henneberger.operator.crd.HealthSpec;
import dev.henneberger.operator.crd.IngestionEndpoint;
import dev.henneberger.operator.crd.IngestionEndpointSpec;
import dev.henneberger.operator.crd.JsonSchema;
import dev.henneberger.operator.crd.JsonSchemaSpec;
import dev.henneberger.operator.crd.JdbcConnection;
import dev.henneberger.operator.crd.JdbcConnectionSpec;
import dev.henneberger.operator.crd.KafkaConnection;
import dev.henneberger.operator.crd.KafkaConnectionSpec;
import dev.henneberger.operator.crd.CursorSigningSpec;
import dev.henneberger.operator.crd.KafkaEvent;
import dev.henneberger.operator.crd.KafkaEventSpec;
import dev.henneberger.operator.crd.KafkaSubscription;
import dev.henneberger.operator.crd.KafkaSubscriptionSpec;
import dev.henneberger.operator.crd.MetricsSpec;
import dev.henneberger.operator.crd.MongoConnection;
import dev.henneberger.operator.crd.MongoConnectionSpec;
import dev.henneberger.operator.crd.GraphQLOperation;
import dev.henneberger.operator.crd.GraphQLOperationSpec;
import dev.henneberger.operator.crd.GraphQLOperationStatus;
import dev.henneberger.operator.crd.McpServer;
import dev.henneberger.operator.crd.McpServerSpec;
import dev.henneberger.operator.crd.McpServerStatus;
import dev.henneberger.operator.crd.ObservabilitySpec;
import dev.henneberger.operator.crd.OtlpMetricsSpec;
import dev.henneberger.operator.crd.OtlpTracingSpec;
import dev.henneberger.operator.crd.ParamSource;
import dev.henneberger.operator.crd.PaginationSpec;
import dev.henneberger.operator.crd.PostgresMutation;
import dev.henneberger.operator.crd.PostgresMutationSpec;
import dev.henneberger.operator.crd.PostgresMutationStatus;
import dev.henneberger.operator.crd.PostgresSubscription;
import dev.henneberger.operator.crd.PostgresSubscriptionFilter;
import dev.henneberger.operator.crd.PostgresSubscriptionSpec;
import dev.henneberger.operator.crd.PostgresSubscriptionStatus;
import dev.henneberger.operator.crd.PythonFunction;
import dev.henneberger.operator.crd.PythonFunctionSpec;
import dev.henneberger.operator.crd.PythonFunctionStatus;
import dev.henneberger.operator.crd.Query;
import dev.henneberger.operator.crd.QueryParam;
import dev.henneberger.operator.crd.QuerySpec;
import dev.henneberger.operator.crd.QueryStatus;
import dev.henneberger.operator.crd.GraphQLSchemaStatus;
import dev.henneberger.operator.crd.RestGraphQLEndpoint;
import dev.henneberger.operator.crd.RestGraphQLEndpointSpec;
import dev.henneberger.operator.crd.RestGraphQLEndpointStatus;
import dev.henneberger.operator.crd.SecretKeyRef;
import dev.henneberger.operator.crd.SamplingSpec;
import dev.henneberger.operator.crd.TracingSpec;
import dev.henneberger.runtime.config.ConnectionConfig;
import dev.henneberger.runtime.config.CursorSigningConfig;
import dev.henneberger.runtime.config.DynamoDbConnectionConfig;
import dev.henneberger.runtime.config.DynamoDbQueryConfig;
import dev.henneberger.runtime.config.DuckDbConnectionConfig;
import dev.henneberger.runtime.config.ElasticsearchConnectionConfig;
import dev.henneberger.runtime.config.ElasticsearchQueryConfig;
import dev.henneberger.runtime.config.JdbcConnectionConfig;
import dev.henneberger.runtime.config.KafkaConnectionConfig;
import dev.henneberger.runtime.config.EnrichFieldConfig;
import dev.henneberger.runtime.config.AddFieldConfig;
import dev.henneberger.runtime.config.IngestionEndpointConfig;
import dev.henneberger.runtime.config.JsonSchemaConfig;
import dev.henneberger.runtime.config.KafkaEventConfig;
import dev.henneberger.runtime.config.HealthCheckConfig;
import dev.henneberger.runtime.config.HealthConfig;
import dev.henneberger.runtime.config.KafkaSubscriptionConfig;
import dev.henneberger.runtime.config.KafkaSubscriptionFilterConfig;
import dev.henneberger.runtime.config.MetricsConfig;
import dev.henneberger.runtime.config.MongoConnectionConfig;
import dev.henneberger.runtime.config.MongoQueryConfig;
import dev.henneberger.runtime.config.ObservabilityConfig;
import dev.henneberger.runtime.config.OtlpMetricsConfig;
import dev.henneberger.runtime.config.OtlpTracingConfig;
import dev.henneberger.runtime.config.PaginationConfig;
import dev.henneberger.runtime.config.ParamSourceConfig;
import dev.henneberger.runtime.config.PostgresMutationConfig;
import dev.henneberger.runtime.config.PostgresSubscriptionConfig;
import dev.henneberger.runtime.config.PostgresSubscriptionFilterConfig;
import dev.henneberger.runtime.config.PythonFunctionConfig;
import dev.henneberger.runtime.config.QueryConfig;
import dev.henneberger.runtime.config.QueryParamConfig;
import dev.henneberger.runtime.config.GraphQLOperationConfig;
import dev.henneberger.runtime.config.McpServerConfig;
import dev.henneberger.runtime.config.RestGraphQLEndpointConfig;
import dev.henneberger.runtime.config.RestParamSourceConfig;
import dev.henneberger.runtime.config.SamplingConfig;
import dev.henneberger.runtime.config.TracingConfig;
import dev.henneberger.runtime.config.AuthConfig;
import dev.henneberger.runtime.config.BasicAuthUserConfig;
import dev.henneberger.runtime.config.JwtAuthConfig;
import dev.henneberger.runtime.config.RuntimeConfig;
import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphQLSchemaResourceManager {

  private static final String DEFAULT_IMAGE = "vertx-graphql:latest";
  private static final String CONFIG_FILENAME = "config.json";
  private static final String CONFIG_PATH = "/config/" + CONFIG_FILENAME;
  private static final Pattern SQL_PARAM = Pattern.compile("\\$(\\d+)");

  private final KubernetesClient client;
  private final ObjectMapper mapper = new ObjectMapper();

  public GraphQLSchemaResourceManager(KubernetesClient client) {
    this.client = client;
  }

  public void ensureResources(GraphQLSchema schema) {
    GraphQLSchemaSpec spec = schema.getSpec();
    if (spec == null || spec.getSchema() == null || spec.getSchema().isBlank()) {
      throw new IllegalStateException("GraphQLSchema spec.schema is required");
    }

    String namespace = schema.getMetadata().getNamespace();
    try {
      RuntimeBuild build = buildRuntimeConfig(namespace, schema);
      String configJson = toJson(build.runtimeConfig);
      String checksum = sha256(configJson);

      ConfigMap configMap = buildConfigMap(schema, configJson);
      Deployment deployment = buildDeployment(schema, checksum, build.secretEnvVars);
      Service service = buildService(schema);

      client.configMaps().inNamespace(namespace).resource(configMap).createOrReplace();
      client.apps().deployments().inNamespace(namespace).resource(deployment).createOrReplace();
      client.services().inNamespace(namespace).resource(service).createOrReplace();
      setSchemaStatus(schema, "Ready");
    } catch (Exception e) {
      setSchemaStatus(schema, "Invalid: " + e.getMessage());
      throw e;
    }
  }

  public void ensureResourcesForSchemas(String namespace, List<String> schemaRefs) {
    if (schemaRefs == null || schemaRefs.isEmpty()) {
      return;
    }
    for (String schemaRef : schemaRefs) {
      if (schemaRef == null || schemaRef.isBlank()) {
        continue;
      }
      GraphQLSchema schema = client.resources(GraphQLSchema.class).inNamespace(namespace).withName(schemaRef).get();
      if (schema == null) {
        throw new IllegalStateException("GraphQLSchema " + schemaRef + " not found");
      }
      ensureResources(schema);
    }
  }

  public void ensureResourcesForAllSchemas(String namespace) {
    List<GraphQLSchema> schemas = client.resources(GraphQLSchema.class).inNamespace(namespace).list().getItems();
    for (GraphQLSchema schema : schemas) {
      ensureResources(schema);
    }
  }

  private RuntimeBuild buildRuntimeConfig(String namespace, GraphQLSchema schema) {
    String schemaName = schema.getMetadata().getName();
    String sdl = schema.getSpec().getSchema();

    List<Query> queries = client.resources(Query.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<Query> schemaQueries = new ArrayList<>();
    for (Query query : queries) {
      QuerySpec querySpec = query.getSpec();
      if (querySpec == null || !hasSchemaRef(querySpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaQueries.add(query);
    }

    List<KafkaEvent> events = client.resources(KafkaEvent.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<KafkaEvent> schemaEvents = new ArrayList<>();
    for (KafkaEvent event : events) {
      KafkaEventSpec eventSpec = event.getSpec();
      if (eventSpec == null || !hasSchemaRef(eventSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaEvents.add(event);
    }

    List<PostgresMutation> postgresMutations = client.resources(PostgresMutation.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<PostgresMutation> schemaPostgresMutations = new ArrayList<>();
    for (PostgresMutation mutation : postgresMutations) {
      PostgresMutationSpec mutationSpec = mutation.getSpec();
      if (mutationSpec == null || !hasSchemaRef(mutationSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaPostgresMutations.add(mutation);
    }

    List<PostgresSubscription> postgresSubscriptions = client.resources(PostgresSubscription.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<PostgresSubscription> schemaPostgresSubscriptions = new ArrayList<>();
    for (PostgresSubscription subscription : postgresSubscriptions) {
      PostgresSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || !hasSchemaRef(subSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaPostgresSubscriptions.add(subscription);
    }

    List<KafkaSubscription> subscriptions = client.resources(KafkaSubscription.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<KafkaSubscription> schemaSubscriptions = new ArrayList<>();
    for (KafkaSubscription subscription : subscriptions) {
      KafkaSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || !hasSchemaRef(subSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaSubscriptions.add(subscription);
    }

    List<IngestionEndpoint> ingestionEndpoints = client.resources(IngestionEndpoint.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<IngestionEndpoint> schemaIngestionEndpoints = new ArrayList<>();
    for (IngestionEndpoint endpoint : ingestionEndpoints) {
      IngestionEndpointSpec endpointSpec = endpoint.getSpec();
      if (endpointSpec == null || !hasSchemaRef(endpointSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaIngestionEndpoints.add(endpoint);
    }

    List<GraphQLOperation> operations = client.resources(GraphQLOperation.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<GraphQLOperation> schemaOperations = new ArrayList<>();
    for (GraphQLOperation operation : operations) {
      GraphQLOperationSpec opSpec = operation.getSpec();
      if (opSpec == null || !hasSchemaRef(opSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaOperations.add(operation);
    }

    List<RestGraphQLEndpoint> restEndpoints = client.resources(RestGraphQLEndpoint.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<RestGraphQLEndpoint> schemaRestEndpoints = new ArrayList<>();
    for (RestGraphQLEndpoint endpoint : restEndpoints) {
      RestGraphQLEndpointSpec epSpec = endpoint.getSpec();
      if (epSpec == null || !hasSchemaRef(epSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaRestEndpoints.add(endpoint);
    }

    List<McpServer> mcpServers = client.resources(McpServer.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    List<McpServer> schemaMcpServers = new ArrayList<>();
    for (McpServer server : mcpServers) {
      McpServerSpec serverSpec = server.getSpec();
      if (serverSpec == null || !hasSchemaRef(serverSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      schemaMcpServers.add(server);
    }

    Map<String, JsonSchema> jsonSchemaMap = new HashMap<>();
    for (JsonSchema schemaRef : client.resources(JsonSchema.class).inNamespace(namespace).list().getItems()) {
      jsonSchemaMap.put(schemaRef.getMetadata().getName(), schemaRef);
    }

    Map<String, Connection> connectionMap = new HashMap<>();
    for (Connection connection : client.resources(Connection.class).inNamespace(namespace).list().getItems()) {
      connectionMap.put(connection.getMetadata().getName(), connection);
    }

    Map<String, DuckDBConnection> duckdbConnectionMap = new HashMap<>();
    for (DuckDBConnection connection : client.resources(DuckDBConnection.class).inNamespace(namespace).list().getItems()) {
      duckdbConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Map<String, MongoConnection> mongoConnectionMap = new HashMap<>();
    for (MongoConnection connection : client.resources(MongoConnection.class).inNamespace(namespace).list().getItems()) {
      mongoConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Map<String, ElasticsearchConnection> elasticsearchConnectionMap = new HashMap<>();
    for (ElasticsearchConnection connection : client.resources(ElasticsearchConnection.class)
      .inNamespace(namespace).list().getItems()) {
      elasticsearchConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Map<String, JdbcConnection> jdbcConnectionMap = new HashMap<>();
    for (JdbcConnection connection : client.resources(JdbcConnection.class).inNamespace(namespace).list().getItems()) {
      jdbcConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Map<String, DynamoDbConnection> dynamodbConnectionMap = new HashMap<>();
    for (DynamoDbConnection connection : client.resources(DynamoDbConnection.class)
      .inNamespace(namespace).list().getItems()) {
      dynamodbConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Set<String> postgresConnectionRefs = new HashSet<>();
    Set<String> duckdbConnectionRefs = new HashSet<>();
    Set<String> jdbcConnectionRefs = new HashSet<>();
    Set<String> mongoConnectionRefs = new HashSet<>();
    Set<String> elasticsearchConnectionRefs = new HashSet<>();
    Set<String> dynamodbConnectionRefs = new HashSet<>();
    for (Query query : schemaQueries) {
      QuerySpec querySpec = query.getSpec();
      if (querySpec == null || querySpec.getConnectionRef() == null || querySpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException("Query " + query.getMetadata().getName() + " missing connectionRef");
      }
      QuerySpec.Engine engine = querySpec.getEngine();
      if (engine == null || engine == QuerySpec.Engine.POSTGRES) {
        postgresConnectionRefs.add(querySpec.getConnectionRef());
      } else if (engine == QuerySpec.Engine.DUCKDB) {
        duckdbConnectionRefs.add(querySpec.getConnectionRef());
      } else if (engine == QuerySpec.Engine.JDBC) {
        jdbcConnectionRefs.add(querySpec.getConnectionRef());
      } else if (engine == QuerySpec.Engine.MONGODB) {
        mongoConnectionRefs.add(querySpec.getConnectionRef());
      } else if (engine == QuerySpec.Engine.ELASTICSEARCH) {
        elasticsearchConnectionRefs.add(querySpec.getConnectionRef());
      } else if (engine == QuerySpec.Engine.DYNAMODB) {
        dynamodbConnectionRefs.add(querySpec.getConnectionRef());
      }
    }
    for (PostgresMutation mutation : schemaPostgresMutations) {
      PostgresMutationSpec mutationSpec = mutation.getSpec();
      if (mutationSpec == null || mutationSpec.getConnectionRef() == null || mutationSpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException(
          "PostgresMutation " + mutation.getMetadata().getName() + " missing connectionRef");
      }
      postgresConnectionRefs.add(mutationSpec.getConnectionRef());
    }
    for (PostgresSubscription subscription : schemaPostgresSubscriptions) {
      PostgresSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || subSpec.getConnectionRef() == null || subSpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException(
          "PostgresSubscription " + subscription.getMetadata().getName() + " missing connectionRef");
      }
      postgresConnectionRefs.add(subSpec.getConnectionRef());
    }

    List<ConnectionConfig> connectionConfigs = new ArrayList<>();
    List<io.fabric8.kubernetes.api.model.EnvVar> secretEnvVars = new ArrayList<>();
    for (String connectionRef : postgresConnectionRefs) {
      Connection connection = connectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("Connection " + connectionRef + " not found");
      }
      ConnectionSpec connectionSpec = connection.getSpec();
      ConnectionConfig config = new ConnectionConfig();
      config.setName(connectionRef);
      config.setHost(connectionSpec.getHost());
      config.setPort(connectionSpec.getPort());
      config.setDatabase(connectionSpec.getDatabase());
      config.setUser(connectionSpec.getUser());
      config.setSsl(Boolean.TRUE.equals(connectionSpec.getSsl()));
      if (connectionSpec.getPool() != null) {
        ConnectionConfig.PoolConfig pool = new ConnectionConfig.PoolConfig();
        pool.setMaxSize(connectionSpec.getPool().getMaxSize());
        pool.setMaxWaitQueueSize(connectionSpec.getPool().getMaxWaitQueueSize());
        pool.setIdleTimeoutSeconds(connectionSpec.getPool().getIdleTimeoutSeconds());
        pool.setConnectionTimeoutSeconds(connectionSpec.getPool().getConnectionTimeoutSeconds());
        config.setPool(pool);
      }

      if (connectionSpec.getPassword() != null && !connectionSpec.getPassword().isBlank()) {
        config.setPassword(connectionSpec.getPassword());
      } else if (connectionSpec.getPasswordSecretRef() != null) {
        String envName = passwordEnvName(connectionRef);
        config.setPasswordEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, connectionSpec.getPasswordSecretRef()));
      }

      connectionConfigs.add(config);
    }

    List<DuckDbConnectionConfig> duckdbConnectionConfigs = new ArrayList<>();
    for (String connectionRef : duckdbConnectionRefs) {
      DuckDBConnection connection = duckdbConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("DuckDBConnection " + connectionRef + " not found");
      }
      DuckDBConnectionSpec connectionSpec = connection.getSpec();
      DuckDbConnectionConfig config = new DuckDbConnectionConfig();
      config.setName(connectionRef);
      config.setDatabase(connectionSpec.getDatabase());
      config.setSettings(connectionSpec.getSettings());
      duckdbConnectionConfigs.add(config);
    }

    List<MongoConnectionConfig> mongoConnectionConfigs = new ArrayList<>();
    for (String connectionRef : mongoConnectionRefs) {
      MongoConnection connection = mongoConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("MongoConnection " + connectionRef + " not found");
      }
      MongoConnectionSpec spec = connection.getSpec();
      MongoConnectionConfig config = new MongoConnectionConfig();
      config.setName(connectionRef);
      config.setHost(spec.getHost());
      config.setPort(spec.getPort());
      config.setDatabase(spec.getDatabase());
      config.setUser(spec.getUser());
      config.setAuthSource(spec.getAuthSource());
      config.setSsl(Boolean.TRUE.equals(spec.getSsl()));
      config.setOptions(spec.getOptions());
      if (spec.getPassword() != null && !spec.getPassword().isBlank()) {
        config.setPassword(spec.getPassword());
      } else if (spec.getPasswordSecretRef() != null) {
        String envName = secretEnvName("MONGO_PASSWORD", connectionRef);
        config.setPasswordEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getPasswordSecretRef()));
      }
      mongoConnectionConfigs.add(config);
    }

    List<ElasticsearchConnectionConfig> elasticsearchConnectionConfigs = new ArrayList<>();
    for (String connectionRef : elasticsearchConnectionRefs) {
      ElasticsearchConnection connection = elasticsearchConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("ElasticsearchConnection " + connectionRef + " not found");
      }
      ElasticsearchConnectionSpec spec = connection.getSpec();
      ElasticsearchConnectionConfig config = new ElasticsearchConnectionConfig();
      config.setName(connectionRef);
      config.setEndpoint(spec.getEndpoint());
      config.setUsername(spec.getUsername());
      if (spec.getPassword() != null && !spec.getPassword().isBlank()) {
        config.setPassword(spec.getPassword());
      } else if (spec.getPasswordSecretRef() != null) {
        String envName = secretEnvName("ELASTIC_PASSWORD", connectionRef);
        config.setPasswordEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getPasswordSecretRef()));
      }
      if (spec.getApiKey() != null && !spec.getApiKey().isBlank()) {
        config.setApiKey(spec.getApiKey());
      } else if (spec.getApiKeySecretRef() != null) {
        String envName = secretEnvName("ELASTIC_API_KEY", connectionRef);
        config.setApiKeyEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getApiKeySecretRef()));
      }
      elasticsearchConnectionConfigs.add(config);
    }

    List<JdbcConnectionConfig> jdbcConnectionConfigs = new ArrayList<>();
    for (String connectionRef : jdbcConnectionRefs) {
      JdbcConnection connection = jdbcConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("JdbcConnection " + connectionRef + " not found");
      }
      JdbcConnectionSpec spec = connection.getSpec();
      JdbcConnectionConfig config = new JdbcConnectionConfig();
      config.setName(connectionRef);
      config.setJdbcUrl(spec.getJdbcUrl());
      config.setDriverClass(spec.getDriverClass());
      config.setUser(spec.getUser());
      if (spec.getPassword() != null && !spec.getPassword().isBlank()) {
        config.setPassword(spec.getPassword());
      } else if (spec.getPasswordSecretRef() != null) {
        String envName = secretEnvName("JDBC_PASSWORD", connectionRef);
        config.setPasswordEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getPasswordSecretRef()));
      }
      jdbcConnectionConfigs.add(config);
    }

    List<DynamoDbConnectionConfig> dynamodbConnectionConfigs = new ArrayList<>();
    for (String connectionRef : dynamodbConnectionRefs) {
      DynamoDbConnection connection = dynamodbConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("DynamoDbConnection " + connectionRef + " not found");
      }
      DynamoDbConnectionSpec spec = connection.getSpec();
      DynamoDbConnectionConfig config = new DynamoDbConnectionConfig();
      config.setName(connectionRef);
      config.setRegion(spec.getRegion());
      config.setEndpoint(spec.getEndpoint());
      if (spec.getAccessKey() != null && !spec.getAccessKey().isBlank()) {
        config.setAccessKey(spec.getAccessKey());
      } else if (spec.getAccessKeySecretRef() != null) {
        String envName = secretEnvName("DDB_ACCESS_KEY", connectionRef);
        config.setAccessKeyEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getAccessKeySecretRef()));
      }
      if (spec.getSecretKey() != null && !spec.getSecretKey().isBlank()) {
        config.setSecretKey(spec.getSecretKey());
      } else if (spec.getSecretKeySecretRef() != null) {
        String envName = secretEnvName("DDB_SECRET_KEY", connectionRef);
        config.setSecretKeyEnv(envName);
        secretEnvVars.add(secretEnvVar(envName, spec.getSecretKeySecretRef()));
      }
      dynamodbConnectionConfigs.add(config);
    }

    Map<String, KafkaConnection> kafkaConnectionMap = new HashMap<>();
    for (KafkaConnection connection : client.resources(KafkaConnection.class).inNamespace(namespace).list().getItems()) {
      kafkaConnectionMap.put(connection.getMetadata().getName(), connection);
    }

    Set<String> kafkaConnectionRefs = new HashSet<>();
    for (KafkaEvent event : schemaEvents) {
      KafkaEventSpec eventSpec = event.getSpec();
      if (eventSpec == null || eventSpec.getConnectionRef() == null || eventSpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException("KafkaEvent " + event.getMetadata().getName() + " missing connectionRef");
      }
      kafkaConnectionRefs.add(eventSpec.getConnectionRef());
    }
    for (KafkaSubscription subscription : schemaSubscriptions) {
      KafkaSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || subSpec.getConnectionRef() == null || subSpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException(
          "KafkaSubscription " + subscription.getMetadata().getName() + " missing connectionRef");
      }
      kafkaConnectionRefs.add(subSpec.getConnectionRef());
    }
    for (IngestionEndpoint endpoint : schemaIngestionEndpoints) {
      IngestionEndpointSpec endpointSpec = endpoint.getSpec();
      if (endpointSpec == null || endpointSpec.getConnectionRef() == null || endpointSpec.getConnectionRef().isBlank()) {
        throw new IllegalStateException(
          "IngestionEndpoint " + endpoint.getMetadata().getName() + " missing connectionRef");
      }
      kafkaConnectionRefs.add(endpointSpec.getConnectionRef());
    }

    List<KafkaConnectionConfig> kafkaConnectionConfigs = new ArrayList<>();
    for (String connectionRef : kafkaConnectionRefs) {
      KafkaConnection connection = kafkaConnectionMap.get(connectionRef);
      if (connection == null || connection.getSpec() == null) {
        throw new IllegalStateException("KafkaConnection " + connectionRef + " not found");
      }
      KafkaConnectionSpec spec = connection.getSpec();
      KafkaConnectionConfig config = new KafkaConnectionConfig();
      config.setName(connectionRef);
      config.setBootstrapServers(spec.getBootstrapServers());
      config.setClientId(spec.getClientId());
      config.setAcks(spec.getAcks());
      config.setLingerMs(spec.getLingerMs());
      config.setBatchSize(spec.getBatchSize());
      kafkaConnectionConfigs.add(config);
    }

    List<QueryConfig> queryConfigs = new ArrayList<>();
    for (Query query : schemaQueries) {
      QuerySpec querySpec = query.getSpec();
      if (querySpec == null) {
        continue;
      }
      QuerySpec.Engine engine = querySpec.getEngine();
      if (engine == null || engine == QuerySpec.Engine.POSTGRES || engine == QuerySpec.Engine.DUCKDB
        || engine == QuerySpec.Engine.JDBC) {
        if (querySpec.getSql() == null || querySpec.getSql().isBlank()) {
          continue;
        }
      } else if (engine == QuerySpec.Engine.MONGODB) {
        if (querySpec.getMongo() == null || querySpec.getMongo().getCollection() == null) {
          continue;
        }
      } else if (engine == QuerySpec.Engine.ELASTICSEARCH) {
        if (querySpec.getElasticsearch() == null || querySpec.getElasticsearch().getIndex() == null) {
          continue;
        }
      } else if (engine == QuerySpec.Engine.DYNAMODB) {
        if (querySpec.getDynamodb() == null || querySpec.getDynamodb().getTable() == null) {
          continue;
        }
      }
      QueryConfig queryConfig = new QueryConfig();
      String fieldName = Optional.ofNullable(querySpec.getFieldName())
        .filter(value -> !value.isBlank())
        .orElse(query.getMetadata().getName());
      String typeName = Optional.ofNullable(querySpec.getTypeName())
        .filter(value -> !value.isBlank())
        .orElse("Query");
      queryConfig.setName(fieldName);
      queryConfig.setTypeName(typeName);
      queryConfig.setSql(querySpec.getSql());
      queryConfig.setResultMode(mapResultMode(querySpec.getResultMode()));
      queryConfig.setEngine(mapEngine(querySpec.getEngine()));
      queryConfig.setDescription(querySpec.getDescription());
      queryConfig.setParams(mapParams(querySpec.getParams()));
      queryConfig.setTimeoutMs(querySpec.getTimeoutMs());
      queryConfig.setConnectionRef(querySpec.getConnectionRef());
      queryConfig.setRequiredRoles(querySpec.getRequiredRoles());
      queryConfig.setRequiredScopes(querySpec.getRequiredScopes());
      queryConfig.setPagination(mapPagination(querySpec.getPagination()));
      queryConfig.setMongo(mapMongoQuery(querySpec.getMongo()));
      queryConfig.setElasticsearch(mapElasticsearchQuery(querySpec.getElasticsearch()));
      queryConfig.setDynamodb(mapDynamoQuery(querySpec.getDynamodb()));
      queryConfigs.add(queryConfig);
    }

    List<KafkaEventConfig> eventConfigs = new ArrayList<>();
    for (KafkaEvent event : schemaEvents) {
      KafkaEventSpec eventSpec = event.getSpec();
      if (eventSpec == null || eventSpec.getTopic() == null) {
        continue;
      }
      KafkaEventConfig config = new KafkaEventConfig();
      String eventName = Optional.ofNullable(eventSpec.getEventName())
        .filter(value -> !value.isBlank())
        .orElse(event.getMetadata().getName());
      config.setName(eventName);
      config.setConnectionRef(eventSpec.getConnectionRef());
      config.setTopic(eventSpec.getTopic());
      config.setPartitionKeyField(eventSpec.getPartitionKeyField());
      config.setPartitionKeyFields(eventSpec.getPartitionKeyFields());
      config.setDescription(eventSpec.getDescription());
      config.setEnrich(mapEnrichFields(eventSpec.getEnrich()));
      config.setRequiredRoles(eventSpec.getRequiredRoles());
      config.setRequiredScopes(eventSpec.getRequiredScopes());
      eventConfigs.add(config);
    }

    List<PostgresMutationConfig> mutationConfigs = new ArrayList<>();
    for (PostgresMutation mutation : schemaPostgresMutations) {
      PostgresMutationSpec mutationSpec = mutation.getSpec();
      if (mutationSpec == null || mutationSpec.getSql() == null) {
        continue;
      }
      PostgresMutationConfig config = new PostgresMutationConfig();
      String mutationName = Optional.ofNullable(mutationSpec.getMutationName())
        .filter(value -> !value.isBlank())
        .orElse(mutation.getMetadata().getName());
      config.setName(mutationName);
      config.setConnectionRef(mutationSpec.getConnectionRef());
      config.setSql(mutationSpec.getSql());
      config.setDescription(mutationSpec.getDescription());
      config.setParams(mapParams(mutationSpec.getParams()));
      config.setEnrich(mapEnrichFields(mutationSpec.getEnrich()));
      config.setTimeoutMs(mutationSpec.getTimeoutMs());
      config.setRequiredRoles(mutationSpec.getRequiredRoles());
      config.setRequiredScopes(mutationSpec.getRequiredScopes());
      mutationConfigs.add(config);
    }

    List<PostgresSubscriptionConfig> postgresSubscriptionConfigs = new ArrayList<>();
    for (PostgresSubscription subscription : schemaPostgresSubscriptions) {
      PostgresSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || subSpec.getTable() == null || subSpec.getTable().isBlank()) {
        continue;
      }
      PostgresSubscriptionConfig config = new PostgresSubscriptionConfig();
      String fieldName = Optional.ofNullable(subSpec.getFieldName())
        .filter(value -> !value.isBlank())
        .orElse(subscription.getMetadata().getName());
      config.setName(subscription.getMetadata().getName());
      config.setFieldName(fieldName);
      config.setConnectionRef(subSpec.getConnectionRef());
      config.setTable(subSpec.getTable());
      config.setOperations(mapPostgresOperations(subSpec.getOperations()));
      config.setFilters(mapPostgresSubscriptionFilters(subSpec.getFilters()));
      config.setRequiredRoles(subSpec.getRequiredRoles());
      config.setRequiredScopes(subSpec.getRequiredScopes());
      postgresSubscriptionConfigs.add(config);
    }

    List<KafkaSubscriptionConfig> subscriptionConfigs = new ArrayList<>();
    for (KafkaSubscription subscription : schemaSubscriptions) {
      KafkaSubscriptionSpec subSpec = subscription.getSpec();
      if (subSpec == null || subSpec.getTopic() == null) {
        continue;
      }
      KafkaSubscriptionConfig config = new KafkaSubscriptionConfig();
      String fieldName = Optional.ofNullable(subSpec.getFieldName())
        .filter(value -> !value.isBlank())
        .orElse(subscription.getMetadata().getName());
      config.setName(subscription.getMetadata().getName());
      config.setFieldName(fieldName);
      config.setConnectionRef(subSpec.getConnectionRef());
      config.setTopic(subSpec.getTopic());
      config.setFormat(subSpec.getFormat());
      config.setFilters(mapSubscriptionFilters(subSpec.getFilters()));
      config.setRequiredRoles(subSpec.getRequiredRoles());
      config.setRequiredScopes(subSpec.getRequiredScopes());
      subscriptionConfigs.add(config);
    }

    List<IngestionEndpointConfig> ingestionEndpointConfigs = new ArrayList<>();
    Set<String> jsonSchemaRefs = new HashSet<>();
    for (IngestionEndpoint endpoint : schemaIngestionEndpoints) {
      IngestionEndpointSpec endpointSpec = endpoint.getSpec();
      if (endpointSpec == null || endpointSpec.getTopic() == null || endpointSpec.getTopic().isBlank()) {
        continue;
      }
      IngestionEndpointConfig config = new IngestionEndpointConfig();
      config.setName(endpoint.getMetadata().getName());
      config.setConnectionRef(endpointSpec.getConnectionRef());
      config.setPath(endpointSpec.getPath());
      config.setMethod(endpointSpec.getMethod());
      config.setContentType(endpointSpec.getContentType());
      config.setMaxBodyBytes(endpointSpec.getMaxBodyBytes());
      config.setRequireJson(endpointSpec.getRequireJson());
      config.setTopic(endpointSpec.getTopic());
      config.setPartitionKeyField(endpointSpec.getPartitionKeyField());
      config.setJsonSchemaRef(endpointSpec.getJsonSchemaRef());
      config.setAddFields(mapAddFields(endpointSpec.getAddFields()));
      config.setResponseCode(endpointSpec.getResponseCode());
      config.setResponseContentType(endpointSpec.getResponseContentType());
      config.setResponseFields(endpointSpec.getResponseFields());
      if (endpointSpec.getJsonSchemaRef() != null && !endpointSpec.getJsonSchemaRef().isBlank()) {
        jsonSchemaRefs.add(endpointSpec.getJsonSchemaRef());
      }
      ingestionEndpointConfigs.add(config);
    }

    List<JsonSchemaConfig> jsonSchemaConfigs = new ArrayList<>();
    for (String jsonSchemaRef : jsonSchemaRefs) {
      JsonSchema schemaRef = jsonSchemaMap.get(jsonSchemaRef);
      if (schemaRef == null || schemaRef.getSpec() == null) {
        throw new IllegalStateException("JsonSchema " + jsonSchemaRef + " not found");
      }
      JsonSchemaSpec schemaSpec = schemaRef.getSpec();
      JsonSchemaConfig config = new JsonSchemaConfig();
      config.setName(jsonSchemaRef);
      config.setDescription(schemaSpec.getDescription());
      config.setSchema(schemaSpec.getSchema());
      jsonSchemaConfigs.add(config);
    }

    List<PythonFunctionConfig> pythonFunctionConfigs = new ArrayList<>();
    List<PythonFunction> pythonFunctions = client.resources(PythonFunction.class)
      .inNamespace(namespace)
      .list()
      .getItems();
    for (PythonFunction fn : pythonFunctions) {
      if (fn == null || fn.getMetadata() == null || fn.getMetadata().getName() == null
        || fn.getMetadata().getName().isBlank()) {
        continue;
      }
      PythonFunctionSpec fnSpec = fn.getSpec();
      if (fnSpec == null || fnSpec.getFunction() == null
        || fnSpec.getFunction().getName() == null || fnSpec.getFunction().getName().isBlank()
        || fnSpec.getFunction().getModule() == null
        || fnSpec.getFunction().getModule().getInline() == null
        || fnSpec.getFunction().getModule().getInline().isBlank()) {
        continue;
      }
      if (!hasSchemaRef(fnSpec.getSchemaRefs(), schemaName)) {
        continue;
      }
      PythonFunctionConfig config = new PythonFunctionConfig();
      config.setName(fn.getMetadata().getName());
      if (fnSpec.getRequirements() != null) {
        PythonFunctionConfig.Requirements requirements = new PythonFunctionConfig.Requirements();
        requirements.setInline(fnSpec.getRequirements().getInline());
        config.setRequirements(requirements);
      }
      PythonFunctionConfig.Function function = new PythonFunctionConfig.Function();
      function.setName(fnSpec.getFunction().getName());
      PythonFunctionConfig.Module module = new PythonFunctionConfig.Module();
      module.setInline(fnSpec.getFunction().getModule().getInline());
      function.setModule(module);
      config.setFunction(function);
      config.setTimeoutMs(fnSpec.getTimeoutMs());
      pythonFunctionConfigs.add(config);
    }

    List<GraphQLOperationConfig> operationConfigs = new ArrayList<>();
    for (GraphQLOperation op : schemaOperations) {
      if (op == null || op.getMetadata() == null || op.getMetadata().getName() == null
        || op.getMetadata().getName().isBlank()) {
        continue;
      }
      GraphQLOperationSpec opSpec = op.getSpec();
      if (opSpec == null || opSpec.getDocument() == null || opSpec.getDocument().isBlank()) {
        continue;
      }
      GraphQLOperationConfig config = new GraphQLOperationConfig();
      config.setName(op.getMetadata().getName());
      config.setDescription(opSpec.getDescription());
      config.setDocument(opSpec.getDocument());
      config.setOperationName(opSpec.getOperationName());
      operationConfigs.add(config);
    }

    List<RestGraphQLEndpointConfig> restEndpointConfigs = new ArrayList<>();
    for (RestGraphQLEndpoint endpoint : schemaRestEndpoints) {
      if (endpoint == null || endpoint.getMetadata() == null || endpoint.getMetadata().getName() == null
        || endpoint.getMetadata().getName().isBlank()) {
        continue;
      }
      RestGraphQLEndpointSpec epSpec = endpoint.getSpec();
      if (epSpec == null || epSpec.getPath() == null || epSpec.getPath().isBlank()) {
        continue;
      }
      RestGraphQLEndpointConfig config = new RestGraphQLEndpointConfig();
      config.setName(endpoint.getMetadata().getName());
      config.setPath(epSpec.getPath());
      config.setMethod(epSpec.getMethod());
      config.setContentType(epSpec.getContentType());
      config.setMaxBodyBytes(epSpec.getMaxBodyBytes());
      config.setRequireJson(epSpec.getRequireJson());
      config.setRequireAuth(epSpec.getRequireAuth());
      config.setOperationRef(epSpec.getOperationRef());
      config.setDocument(epSpec.getDocument());
      config.setOperationName(epSpec.getOperationName());
      if (epSpec.getVariables() != null && !epSpec.getVariables().isEmpty()) {
        List<RestGraphQLEndpointConfig.Variable> vars = new ArrayList<>();
        for (RestGraphQLEndpointSpec.RestVariable var : epSpec.getVariables()) {
          if (var == null || var.getName() == null || var.getName().isBlank() || var.getSource() == null) {
            continue;
          }
          RestGraphQLEndpointConfig.Variable mapped = new RestGraphQLEndpointConfig.Variable();
          mapped.setName(var.getName());
          RestParamSourceConfig src = new RestParamSourceConfig();
          src.setKind(var.getSource().getKind() == null ? null
            : RestParamSourceConfig.Kind.valueOf(var.getSource().getKind().name()));
          src.setName(var.getSource().getName());
          mapped.setSource(src);
          vars.add(mapped);
        }
        config.setVariables(vars.isEmpty() ? null : vars);
      }
      restEndpointConfigs.add(config);
    }

    List<McpServerConfig> mcpServerConfigs = new ArrayList<>();
    for (McpServer server : schemaMcpServers) {
      if (server == null || server.getMetadata() == null || server.getMetadata().getName() == null
        || server.getMetadata().getName().isBlank()) {
        continue;
      }
      McpServerSpec spec = server.getSpec();
      if (spec == null) {
        continue;
      }
      McpServerConfig cfg = new McpServerConfig();
      cfg.setName(server.getMetadata().getName());
      cfg.setPath(spec.getPath());
      cfg.setRequireAuth(spec.getRequireAuth());
      cfg.setOperationRefs(spec.getOperationRefs());
      cfg.setToolNameStrategy(spec.getToolNameStrategy() == null ? null
        : McpServerConfig.ToolNameStrategy.valueOf(spec.getToolNameStrategy().name()));
      mcpServerConfigs.add(cfg);
    }

    validateAndUpdateStatuses(schema, sdl, schemaQueries, schemaPostgresMutations, pythonFunctionConfigs,
      schemaOperations, schemaRestEndpoints, schemaMcpServers);

    RuntimeConfig runtimeConfig = new RuntimeConfig();
    runtimeConfig.setSchema(sdl);
    runtimeConfig.setConnections(connectionConfigs);
    runtimeConfig.setDuckdbConnections(duckdbConnectionConfigs);
    runtimeConfig.setMongoConnections(mongoConnectionConfigs);
    runtimeConfig.setElasticsearchConnections(elasticsearchConnectionConfigs);
    runtimeConfig.setJdbcConnections(jdbcConnectionConfigs);
    runtimeConfig.setDynamodbConnections(dynamodbConnectionConfigs);
    runtimeConfig.setQueries(queryConfigs);
    runtimeConfig.setPythonFunctions(pythonFunctionConfigs.isEmpty() ? null : pythonFunctionConfigs);
    runtimeConfig.setKafkaConnections(kafkaConnectionConfigs);
    runtimeConfig.setKafkaEvents(eventConfigs);
    runtimeConfig.setPostgresMutations(mutationConfigs);
    runtimeConfig.setPostgresSubscriptions(postgresSubscriptionConfigs);
    runtimeConfig.setKafkaSubscriptions(subscriptionConfigs);
    runtimeConfig.setIngestionEndpoints(ingestionEndpointConfigs);
    runtimeConfig.setJsonSchemas(jsonSchemaConfigs);
    runtimeConfig.setGraphQLOperations(operationConfigs.isEmpty() ? null : operationConfigs);
    runtimeConfig.setRestGraphQLEndpoints(restEndpointConfigs.isEmpty() ? null : restEndpointConfigs);
    runtimeConfig.setMcpServers(mcpServerConfigs.isEmpty() ? null : mcpServerConfigs);
    runtimeConfig.setAuth(mapAuth(schema.getSpec().getAuth()));
    runtimeConfig.setObservability(mapObservability(schema.getSpec().getObservability()));

    return new RuntimeBuild(runtimeConfig, secretEnvVars);
  }

  private AuthConfig mapAuth(dev.henneberger.operator.crd.AuthSpec spec) {
    if (spec == null) {
      return null;
    }
    AuthConfig config = new AuthConfig();
    config.setType(spec.getType() == null ? AuthConfig.Type.NONE : AuthConfig.Type.valueOf(spec.getType().name()));
    config.setRequired(spec.getRequired());
    config.setTokenHeader(spec.getTokenHeader());
    config.setTokenCookie(spec.getTokenCookie());
    config.setTokenQueryParam(spec.getTokenQueryParam());
    if (spec.getJwt() != null) {
      dev.henneberger.operator.crd.JwtAuthSpec jwtSpec = spec.getJwt();
      JwtAuthConfig jwtConfig = new JwtAuthConfig();
      jwtConfig.setJwksUrl(jwtSpec.getJwksUrl());
      jwtConfig.setIssuer(jwtSpec.getIssuer());
      jwtConfig.setAudiences(jwtSpec.getAudiences());
      jwtConfig.setSharedSecret(jwtSpec.getSharedSecret());
      jwtConfig.setSharedSecretEnv(jwtSpec.getSharedSecretEnv());
      jwtConfig.setJwksCacheSeconds(jwtSpec.getJwksCacheSeconds());
      jwtConfig.setClockSkewSeconds(jwtSpec.getClockSkewSeconds());
      config.setJwt(jwtConfig);
    }
    if (spec.getBasicUsers() != null) {
      List<BasicAuthUserConfig> users = new ArrayList<>();
      for (dev.henneberger.operator.crd.BasicAuthUserSpec userSpec : spec.getBasicUsers()) {
        if (userSpec == null || userSpec.getUsername() == null || userSpec.getUsername().isBlank()) {
          continue;
        }
        BasicAuthUserConfig user = new BasicAuthUserConfig();
        user.setUsername(userSpec.getUsername());
        user.setPassword(userSpec.getPassword());
        user.setRoles(userSpec.getRoles());
        users.add(user);
      }
      config.setBasicUsers(users.isEmpty() ? null : users);
    }
    return config;
  }

  private ObservabilityConfig mapObservability(ObservabilitySpec spec) {
    if (spec == null) {
      return null;
    }
    ObservabilityConfig config = new ObservabilityConfig();
    config.setEnabled(spec.getEnabled());
    config.setMetrics(mapMetrics(spec.getMetrics()));
    config.setTracing(mapTracing(spec.getTracing()));
    config.setHealth(mapHealth(spec.getHealth()));
    return config;
  }

  private MetricsConfig mapMetrics(MetricsSpec spec) {
    if (spec == null) {
      return null;
    }
    MetricsConfig config = new MetricsConfig();
    config.setEnabled(spec.getEnabled());
    config.setBackend(spec.getBackend() == null ? MetricsConfig.Backend.PROMETHEUS
      : MetricsConfig.Backend.valueOf(spec.getBackend().name()));
    config.setEndpoint(spec.getEndpoint());
    config.setJvm(spec.getJvm());
    config.setHttpServer(spec.getHttpServer());
    config.setHttpClient(spec.getHttpClient());
    config.setLabels(spec.getLabels());
    config.setOtlp(mapOtlpMetrics(spec.getOtlp()));
    return config;
  }

  private OtlpMetricsConfig mapOtlpMetrics(OtlpMetricsSpec spec) {
    if (spec == null) {
      return null;
    }
    OtlpMetricsConfig config = new OtlpMetricsConfig();
    config.setEndpoint(spec.getEndpoint());
    config.setHeaders(spec.getHeaders());
    config.setStepSeconds(spec.getStepSeconds());
    return config;
  }

  private TracingConfig mapTracing(TracingSpec spec) {
    if (spec == null) {
      return null;
    }
    TracingConfig config = new TracingConfig();
    config.setEnabled(spec.getEnabled());
    config.setExporter(spec.getExporter() == null ? TracingConfig.Exporter.OTLP
      : TracingConfig.Exporter.valueOf(spec.getExporter().name()));
    config.setServiceName(spec.getServiceName());
    config.setOtlp(mapOtlpTracing(spec.getOtlp()));
    config.setSampling(mapSampling(spec.getSampling()));
    return config;
  }

  private OtlpTracingConfig mapOtlpTracing(OtlpTracingSpec spec) {
    if (spec == null) {
      return null;
    }
    OtlpTracingConfig config = new OtlpTracingConfig();
    config.setEndpoint(spec.getEndpoint());
    config.setProtocol(spec.getProtocol() == null ? OtlpTracingConfig.Protocol.GRPC
      : OtlpTracingConfig.Protocol.valueOf(spec.getProtocol().name()));
    config.setHeaders(spec.getHeaders());
    return config;
  }

  private SamplingConfig mapSampling(SamplingSpec spec) {
    if (spec == null) {
      return null;
    }
    SamplingConfig config = new SamplingConfig();
    config.setType(spec.getType() == null ? SamplingConfig.Type.ALWAYS_ON
      : SamplingConfig.Type.valueOf(spec.getType().name()));
    config.setValue(spec.getValue());
    return config;
  }

  private HealthConfig mapHealth(HealthSpec spec) {
    if (spec == null) {
      return null;
    }
    HealthConfig config = new HealthConfig();
    config.setEnabled(spec.getEnabled());
    config.setEndpoint(spec.getEndpoint());
    if (spec.getChecks() != null) {
      List<HealthCheckConfig> checks = new ArrayList<>();
      for (HealthCheckSpec checkSpec : spec.getChecks()) {
        if (checkSpec == null) {
          continue;
        }
        HealthCheckConfig check = new HealthCheckConfig();
        check.setName(checkSpec.getName());
        check.setType(checkSpec.getType() == null ? null
          : HealthCheckConfig.Type.valueOf(checkSpec.getType().name()));
        check.setConnectionRef(checkSpec.getConnectionRef());
        checks.add(check);
      }
      config.setChecks(checks.isEmpty() ? null : checks);
    }
    return config;
  }

  private QueryConfig.ResultMode mapResultMode(QuerySpec.ResultMode resultMode) {
    if (resultMode == null) {
      return QueryConfig.ResultMode.LIST;
    }
    return QueryConfig.ResultMode.valueOf(resultMode.name());
  }

  private QueryConfig.Engine mapEngine(QuerySpec.Engine engine) {
    if (engine == null) {
      return QueryConfig.Engine.POSTGRES;
    }
    return QueryConfig.Engine.valueOf(engine.name());
  }

  private List<QueryParamConfig> mapParams(List<dev.henneberger.operator.crd.QueryParam> params) {
    if (params == null) {
      return null;
    }
    List<QueryParamConfig> mapped = new ArrayList<>();
    for (dev.henneberger.operator.crd.QueryParam param : params) {
      if (param == null || param.getSource() == null) {
        continue;
      }
      QueryParamConfig config = new QueryParamConfig();
      config.setIndex(param.getIndex());
      ParamSourceConfig sourceConfig = new ParamSourceConfig();
      sourceConfig.setKind(ParamSourceConfig.Kind.valueOf(param.getSource().getKind().name()));
      sourceConfig.setName(param.getSource().getName());
      if (param.getSource().getPython() != null) {
        ParamSourceConfig.PythonInvocation py = new ParamSourceConfig.PythonInvocation();
        py.setFunctionRef(param.getSource().getPython().getFunctionRef());
        py.setTimeoutMs(param.getSource().getPython().getTimeoutMs());
        if (param.getSource().getPython().getArgs() != null) {
          List<ParamSourceConfig> args = new ArrayList<>();
          for (dev.henneberger.operator.crd.ParamSource arg : param.getSource().getPython().getArgs()) {
            if (arg == null || arg.getKind() == null) {
              continue;
            }
            ParamSourceConfig argCfg = new ParamSourceConfig();
            argCfg.setKind(ParamSourceConfig.Kind.valueOf(arg.getKind().name()));
            argCfg.setName(arg.getName());
            if (arg.getPython() != null) {
              ParamSourceConfig.PythonInvocation nested = new ParamSourceConfig.PythonInvocation();
              nested.setFunctionRef(arg.getPython().getFunctionRef());
              nested.setTimeoutMs(arg.getPython().getTimeoutMs());
              argCfg.setPython(nested);
            }
            args.add(argCfg);
          }
          py.setArgs(args.isEmpty() ? null : args);
        }
        sourceConfig.setPython(py);
      }
      config.setSource(sourceConfig);
      mapped.add(config);
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private MongoQueryConfig mapMongoQuery(dev.henneberger.operator.crd.MongoQuerySpec spec) {
    if (spec == null) {
      return null;
    }
    MongoQueryConfig config = new MongoQueryConfig();
    config.setCollection(spec.getCollection());
    config.setPipeline(spec.getPipeline());
    return config;
  }

  private ElasticsearchQueryConfig mapElasticsearchQuery(dev.henneberger.operator.crd.ElasticsearchQuerySpec spec) {
    if (spec == null) {
      return null;
    }
    ElasticsearchQueryConfig config = new ElasticsearchQueryConfig();
    config.setIndex(spec.getIndex());
    config.setQuery(spec.getQuery());
    return config;
  }

  private DynamoDbQueryConfig mapDynamoQuery(dev.henneberger.operator.crd.DynamoDbQuerySpec spec) {
    if (spec == null) {
      return null;
    }
    DynamoDbQueryConfig config = new DynamoDbQueryConfig();
    config.setTable(spec.getTable());
    config.setKeyConditionExpression(spec.getKeyConditionExpression());
    config.setExpressionAttributeNames(spec.getExpressionAttributeNames());
    config.setExpressionAttributeValues(spec.getExpressionAttributeValues());
    config.setIndexName(spec.getIndexName());
    config.setLimit(spec.getLimit());
    return config;
  }

  private PaginationConfig mapPagination(PaginationSpec spec) {
    if (spec == null) {
      return null;
    }
    PaginationConfig config = new PaginationConfig();
    config.setMode(mapPaginationMode(spec.getMode()));
    config.setFirstArg(spec.getFirstArg());
    config.setAfterArg(spec.getAfterArg());
    config.setMaxPageSize(spec.getMaxPageSize());
    config.setFetchExtra(spec.getFetchExtra());
    config.setCursorFields(spec.getCursorFields());
    config.setEdgesFieldName(spec.getEdgesFieldName());
    config.setNodeFieldName(spec.getNodeFieldName());
    config.setCursorFieldName(spec.getCursorFieldName());
    config.setPageInfoFieldName(spec.getPageInfoFieldName());
    config.setEndCursorFieldName(spec.getEndCursorFieldName());
    config.setHasNextPageFieldName(spec.getHasNextPageFieldName());
    config.setSigning(mapSigning(spec.getSigning()));
    return config;
  }

  private PaginationConfig.Mode mapPaginationMode(PaginationSpec.Mode mode) {
    if (mode == null) {
      return PaginationConfig.Mode.CURSOR;
    }
    switch (mode) {
      case CURSOR:
      default:
        return PaginationConfig.Mode.CURSOR;
    }
  }

  private CursorSigningConfig mapSigning(CursorSigningSpec spec) {
    if (spec == null) {
      return null;
    }
    CursorSigningConfig config = new CursorSigningConfig();
    config.setMethod(mapSigningMethod(spec.getMethod()));
    config.setSharedSecret(spec.getSharedSecret());
    config.setSharedSecretEnv(spec.getSharedSecretEnv());
    config.setUseAuthJwtSecret(spec.getUseAuthJwtSecret());
    return config;
  }

  private CursorSigningConfig.Method mapSigningMethod(CursorSigningSpec.Method method) {
    if (method == null) {
      return CursorSigningConfig.Method.HMAC;
    }
    switch (method) {
      case JWT:
        return CursorSigningConfig.Method.JWT;
      case HMAC:
      default:
        return CursorSigningConfig.Method.HMAC;
    }
  }

  private List<KafkaSubscriptionFilterConfig> mapSubscriptionFilters(
      List<dev.henneberger.operator.crd.KafkaSubscriptionFilter> filters) {
    if (filters == null) {
      return null;
    }
    List<KafkaSubscriptionFilterConfig> mapped = new ArrayList<>();
    for (dev.henneberger.operator.crd.KafkaSubscriptionFilter filter : filters) {
      if (filter == null || filter.getField() == null || filter.getField().isBlank()) {
        continue;
      }
      if (filter.getSource() == null || filter.getSource().getKind() == null) {
        continue;
      }
      KafkaSubscriptionFilterConfig config = new KafkaSubscriptionFilterConfig();
      config.setField(filter.getField());
      ParamSourceConfig sourceConfig = new ParamSourceConfig();
      sourceConfig.setKind(ParamSourceConfig.Kind.valueOf(filter.getSource().getKind().name()));
      sourceConfig.setName(filter.getSource().getName());
      config.setSource(sourceConfig);
      mapped.add(config);
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private List<PostgresSubscriptionFilterConfig> mapPostgresSubscriptionFilters(
      List<PostgresSubscriptionFilter> filters) {
    if (filters == null) {
      return null;
    }
    List<PostgresSubscriptionFilterConfig> mapped = new ArrayList<>();
    for (PostgresSubscriptionFilter filter : filters) {
      if (filter == null || filter.getField() == null || filter.getField().isBlank()) {
        continue;
      }
      if (filter.getSource() == null || filter.getSource().getKind() == null) {
        continue;
      }
      PostgresSubscriptionFilterConfig config = new PostgresSubscriptionFilterConfig();
      config.setField(filter.getField());
      ParamSourceConfig sourceConfig = new ParamSourceConfig();
      sourceConfig.setKind(ParamSourceConfig.Kind.valueOf(filter.getSource().getKind().name()));
      sourceConfig.setName(filter.getSource().getName());
      config.setSource(sourceConfig);
      mapped.add(config);
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private List<PostgresSubscriptionConfig.Operation> mapPostgresOperations(
      List<PostgresSubscriptionSpec.Operation> operations) {
    if (operations == null || operations.isEmpty()) {
      return null;
    }
    List<PostgresSubscriptionConfig.Operation> mapped = new ArrayList<>();
    for (PostgresSubscriptionSpec.Operation operation : operations) {
      if (operation == null) {
        continue;
      }
      mapped.add(PostgresSubscriptionConfig.Operation.valueOf(operation.name()));
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private ConfigMap buildConfigMap(GraphQLSchema schema, String configJson) {
    Map<String, String> labels = labelsFor(schema.getMetadata().getName());
    return new ConfigMapBuilder()
      .withNewMetadata()
      .withName(configMapName(schema.getMetadata().getName()))
      .withNamespace(schema.getMetadata().getNamespace())
      .withLabels(labels)
      .withOwnerReferences(ownerReference(schema))
      .endMetadata()
      .addToData(CONFIG_FILENAME, configJson)
      .build();
  }

  private Deployment buildDeployment(GraphQLSchema schema,
                                     String checksum,
                                     List<io.fabric8.kubernetes.api.model.EnvVar> secretEnvVars) {
    GraphQLSchemaSpec spec = schema.getSpec();
    String name = schema.getMetadata().getName();
    String namespace = schema.getMetadata().getNamespace();
    Map<String, String> labels = labelsFor(name);

    String image = Optional.ofNullable(spec.getImage()).filter(value -> !value.isBlank()).orElse(DEFAULT_IMAGE);
    int replicas = Optional.ofNullable(spec.getReplicas()).orElse(1);
    int servicePort = Optional.ofNullable(spec.getServicePort()).orElse(8080);

    List<io.fabric8.kubernetes.api.model.EnvVar> envVars = new ArrayList<>();
    envVars.add(new EnvVarBuilder().withName("VERTX_MODE").withValue("runtime").build());
    envVars.add(new EnvVarBuilder().withName("CONFIG_PATH").withValue(CONFIG_PATH).build());
    envVars.add(new EnvVarBuilder().withName("HTTP_PORT").withValue(String.valueOf(servicePort)).build());

    envVars.addAll(secretEnvVars);

    return new DeploymentBuilder()
      .withNewMetadata()
      .withName(deploymentName(name))
      .withNamespace(namespace)
      .withLabels(labels)
      .withOwnerReferences(ownerReference(schema))
      .endMetadata()
      .withNewSpec()
      .withReplicas(replicas)
      .withNewSelector()
      .addToMatchLabels(labels)
      .endSelector()
      .withNewTemplate()
      .withNewMetadata()
      .addToLabels(labels)
      .addToAnnotations("vertx.henneberger.dev/config-checksum", checksum)
      .endMetadata()
      .withNewSpec()
      .addNewContainer()
      .withName("vertx-graphql")
      .withImage(image)
      .withImagePullPolicy("IfNotPresent")
      .addNewPort()
      .withContainerPort(servicePort)
      .endPort()
      .withEnv(envVars)
      .addNewVolumeMount()
      .withName("config")
      .withMountPath("/config")
      .endVolumeMount()
      .endContainer()
      .addNewVolume()
      .withName("config")
      .withNewConfigMap()
      .withName(configMapName(name))
      .endConfigMap()
      .endVolume()
      .endSpec()
      .endTemplate()
      .endSpec()
      .build();
  }

  private Service buildService(GraphQLSchema schema) {
    String name = schema.getMetadata().getName();
    String namespace = schema.getMetadata().getNamespace();
    int servicePort = Optional.ofNullable(schema.getSpec().getServicePort()).orElse(8080);
    Map<String, String> labels = labelsFor(name);

    return new ServiceBuilder()
      .withNewMetadata()
      .withName(serviceName(name))
      .withNamespace(namespace)
      .withLabels(labels)
      .withOwnerReferences(ownerReference(schema))
      .endMetadata()
      .withNewSpec()
      .withSelector(labels)
      .addNewPort()
      .withName("http")
      .withPort(servicePort)
      .withTargetPort(new io.fabric8.kubernetes.api.model.IntOrString(servicePort))
      .endPort()
      .endSpec()
      .build();
  }

  private OwnerReference ownerReference(GraphQLSchema schema) {
    return new OwnerReferenceBuilder()
      .withApiVersion(schema.getApiVersion())
      .withKind(schema.getKind())
      .withName(schema.getMetadata().getName())
      .withUid(schema.getMetadata().getUid())
      .withController(true)
      .withBlockOwnerDeletion(true)
      .build();
  }

  private Map<String, String> labelsFor(String schemaName) {
    Map<String, String> labels = new HashMap<>();
    labels.put("app", "vertx-graphql");
    labels.put("vertx.henneberger.dev/schema", schemaName);
    return labels;
  }

  private String configMapName(String schemaName) {
    return schemaName + "-config";
  }

  private String deploymentName(String schemaName) {
    return schemaName + "-graphql";
  }

  private String serviceName(String schemaName) {
    return schemaName + "-graphql";
  }

  private boolean hasSchemaRef(List<String> refs, String schemaName) {
    if (refs == null || refs.isEmpty()) {
      return false;
    }
    for (String ref : refs) {
      if (schemaName.equals(ref)) {
        return true;
      }
    }
    return false;
  }

  private String toJson(RuntimeConfig config) {
    try {
      return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(config);
    } catch (JsonProcessingException e) {
      throw new IllegalStateException("Failed to serialize runtime config", e);
    }
  }

  private String sha256(String payload) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] encoded = digest.digest(payload.getBytes(StandardCharsets.UTF_8));
      StringBuilder builder = new StringBuilder(encoded.length * 2);
      for (byte value : encoded) {
        builder.append(String.format(Locale.ROOT, "%02x", value));
      }
      return builder.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("Missing SHA-256", e);
    }
  }

  private String passwordEnvName(String connectionName) {
    StringBuilder builder = new StringBuilder("PG_PASSWORD_");
    for (int i = 0; i < connectionName.length(); i++) {
      char ch = connectionName.charAt(i);
      if (ch >= 'a' && ch <= 'z') {
        builder.append((char) (ch - 32));
      } else if ((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
        builder.append(ch);
      } else {
        builder.append('_');
      }
    }
    return builder.toString();
  }

  private String secretEnvName(String prefix, String connectionName) {
    StringBuilder builder = new StringBuilder(prefix).append("_");
    for (int i = 0; i < connectionName.length(); i++) {
      char ch = connectionName.charAt(i);
      if (ch >= 'a' && ch <= 'z') {
        builder.append((char) (ch - 32));
      } else if ((ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9')) {
        builder.append(ch);
      } else {
        builder.append('_');
      }
    }
    return builder.toString();
  }

  private io.fabric8.kubernetes.api.model.EnvVar secretEnvVar(String name, SecretKeyRef ref) {
    return new EnvVarBuilder()
      .withName(name)
      .withNewValueFrom()
      .withNewSecretKeyRef(ref.getKey(), ref.getName(), false)
      .endValueFrom()
      .build();
  }

  private static final class RuntimeBuild {
    private final RuntimeConfig runtimeConfig;
    private final List<io.fabric8.kubernetes.api.model.EnvVar> secretEnvVars;

    private RuntimeBuild(RuntimeConfig runtimeConfig,
                         List<io.fabric8.kubernetes.api.model.EnvVar> secretEnvVars) {
      this.runtimeConfig = runtimeConfig;
      this.secretEnvVars = secretEnvVars;
    }
  }

  private List<EnrichFieldConfig> mapEnrichFields(List<dev.henneberger.operator.crd.EnrichField> fields) {
    if (fields == null) {
      return null;
    }
    List<EnrichFieldConfig> mapped = new ArrayList<>();
    for (dev.henneberger.operator.crd.EnrichField field : fields) {
      if (field == null || field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      EnrichFieldConfig config = new EnrichFieldConfig();
      config.setName(field.getName());
      config.setType(field.getType());
      config.setFn(field.getFn());
      mapped.add(config);
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private List<AddFieldConfig> mapAddFields(List<dev.henneberger.operator.crd.AddField> fields) {
    if (fields == null) {
      return null;
    }
    List<AddFieldConfig> mapped = new ArrayList<>();
    for (dev.henneberger.operator.crd.AddField field : fields) {
      if (field == null || field.getName() == null || field.getName().isBlank()) {
        continue;
      }
      AddFieldConfig config = new AddFieldConfig();
      config.setName(field.getName());
      config.setType(field.getType());
      mapped.add(config);
    }
    return mapped.isEmpty() ? null : mapped;
  }

  private void setSchemaStatus(GraphQLSchema schema, String value) {
    if (schema == null) {
      return;
    }
    GraphQLSchemaStatus status = schema.getStatus();
    if (status == null) {
      status = new GraphQLSchemaStatus();
      schema.setStatus(status);
    }
    status.setStatus(value);
    client.resource(schema).inNamespace(schema.getMetadata().getNamespace()).updateStatus();
  }

  private void setQueryStatus(Query query, String value) {
    if (query == null) {
      return;
    }
    QueryStatus status = query.getStatus();
    if (status == null) {
      status = new QueryStatus();
      query.setStatus(status);
    }
    status.setStatus(value);
    client.resource(query).inNamespace(query.getMetadata().getNamespace()).updateStatus();
  }

  private void setMutationStatus(PostgresMutation mutation, String value) {
    if (mutation == null) {
      return;
    }
    PostgresMutationStatus status = mutation.getStatus();
    if (status == null) {
      status = new PostgresMutationStatus();
      mutation.setStatus(status);
    }
    status.setStatus(value);
    client.resource(mutation).inNamespace(mutation.getMetadata().getNamespace()).updateStatus();
  }

  private void setOperationStatus(GraphQLOperation operation, String value) {
    if (operation == null) {
      return;
    }
    GraphQLOperationStatus status = operation.getStatus();
    if (status == null) {
      status = new GraphQLOperationStatus();
      operation.setStatus(status);
    }
    status.setStatus(value);
    client.resource(operation).inNamespace(operation.getMetadata().getNamespace()).updateStatus();
  }

  private void setRestEndpointStatus(RestGraphQLEndpoint endpoint, String value) {
    if (endpoint == null) {
      return;
    }
    RestGraphQLEndpointStatus status = endpoint.getStatus();
    if (status == null) {
      status = new RestGraphQLEndpointStatus();
      endpoint.setStatus(status);
    }
    status.setStatus(value);
    client.resource(endpoint).inNamespace(endpoint.getMetadata().getNamespace()).updateStatus();
  }

  private void setMcpServerStatus(McpServer server, String value) {
    if (server == null) {
      return;
    }
    McpServerStatus status = server.getStatus();
    if (status == null) {
      status = new McpServerStatus();
      server.setStatus(status);
    }
    status.setStatus(value);
    client.resource(server).inNamespace(server.getMetadata().getNamespace()).updateStatus();
  }

  private void validateAndUpdateStatuses(GraphQLSchema schema,
                                         String sdl,
                                         List<Query> queries,
                                         List<PostgresMutation> mutations,
                                         List<PythonFunctionConfig> pythonFunctions,
                                         List<GraphQLOperation> operations,
                                         List<RestGraphQLEndpoint> restEndpoints,
                                         List<McpServer> mcpServers) {
    // Lightweight, reconcile-time validation. When invalid, update CR status with a concrete message.
    Map<String, String> errors = new LinkedHashMap<>();

    graphql.schema.idl.SchemaParser parser = new graphql.schema.idl.SchemaParser();
    graphql.schema.idl.TypeDefinitionRegistry registry = parser.parse(sdl);

    Set<String> pythonNames = new HashSet<>();
    if (pythonFunctions != null) {
      for (PythonFunctionConfig fn : pythonFunctions) {
        if (fn != null && fn.getName() != null && !fn.getName().isBlank()) {
          pythonNames.add(fn.getName());
        }
      }
    }

    Map<String, GraphQLOperationSpec> opIndex = new HashMap<>();
    if (operations != null) {
      for (GraphQLOperation op : operations) {
        if (op == null || op.getMetadata() == null || op.getMetadata().getName() == null) {
          continue;
        }
        if (op.getSpec() != null) {
          opIndex.put(op.getMetadata().getName(), op.getSpec());
        }
      }
    }

    if (queries != null) {
      for (Query query : queries) {
        if (query == null || query.getMetadata() == null || query.getMetadata().getName() == null) {
          continue;
        }
        try {
          validateQueryResource(registry, query, pythonNames);
          setQueryStatus(query, "Ready");
        } catch (Exception e) {
          String msg = "Invalid: " + e.getMessage();
          setQueryStatus(query, msg);
          errors.put("Query/" + query.getMetadata().getName(), msg);
        }
      }
    }

    if (mutations != null) {
      for (PostgresMutation mutation : mutations) {
        if (mutation == null || mutation.getMetadata() == null || mutation.getMetadata().getName() == null) {
          continue;
        }
        try {
          validatePostgresMutationResource(registry, mutation, pythonNames);
          setMutationStatus(mutation, "Ready");
        } catch (Exception e) {
          String msg = "Invalid: " + e.getMessage();
          setMutationStatus(mutation, msg);
          errors.put("PostgresMutation/" + mutation.getMetadata().getName(), msg);
        }
      }
    }

    if (operations != null) {
      graphql.parser.Parser docParser = new graphql.parser.Parser();
      for (GraphQLOperation op : operations) {
        if (op == null || op.getMetadata() == null || op.getMetadata().getName() == null) {
          continue;
        }
        try {
          GraphQLOperationSpec spec = op.getSpec();
          if (spec == null || spec.getDocument() == null || spec.getDocument().isBlank()) {
            throw new IllegalStateException("spec.document is required");
          }
          docParser.parseDocument(spec.getDocument());
          setOperationStatus(op, "Ready");
        } catch (Exception e) {
          String msg = "Invalid: " + e.getMessage();
          setOperationStatus(op, msg);
          errors.put("GraphQLOperation/" + op.getMetadata().getName(), msg);
        }
      }
    }

    if (restEndpoints != null) {
      graphql.parser.Parser docParser = new graphql.parser.Parser();
      for (RestGraphQLEndpoint endpoint : restEndpoints) {
        if (endpoint == null || endpoint.getMetadata() == null || endpoint.getMetadata().getName() == null) {
          continue;
        }
        try {
          RestGraphQLEndpointSpec spec = endpoint.getSpec();
          if (spec == null || spec.getPath() == null || spec.getPath().isBlank()) {
            throw new IllegalStateException("spec.path is required");
          }
          String doc = null;
          if (spec.getDocument() != null && !spec.getDocument().isBlank()) {
            doc = spec.getDocument();
          } else if (spec.getOperationRef() != null && !spec.getOperationRef().isBlank()) {
            GraphQLOperationSpec op = opIndex.get(spec.getOperationRef());
            if (op == null) {
              throw new IllegalStateException("operationRef '" + spec.getOperationRef() + "' not found");
            }
            doc = op.getDocument();
          } else {
            throw new IllegalStateException("spec.document or spec.operationRef is required");
          }
          docParser.parseDocument(doc);
          setRestEndpointStatus(endpoint, "Ready");
        } catch (Exception e) {
          String msg = "Invalid: " + e.getMessage();
          setRestEndpointStatus(endpoint, msg);
          errors.put("RestGraphQLEndpoint/" + endpoint.getMetadata().getName(), msg);
        }
      }
    }

    if (mcpServers != null) {
      for (McpServer server : mcpServers) {
        if (server == null || server.getMetadata() == null || server.getMetadata().getName() == null) {
          continue;
        }
        try {
          McpServerSpec spec = server.getSpec();
          if (spec == null) {
            throw new IllegalStateException("spec is required");
          }
          String path = spec.getPath();
          if (path != null && !path.isBlank() && !path.trim().startsWith("/")) {
            throw new IllegalStateException("spec.path must start with '/'");
          }
          if (spec.getOperationRefs() != null) {
            for (String opRef : spec.getOperationRefs()) {
              if (opRef == null || opRef.isBlank()) {
                continue;
              }
              if (!opIndex.containsKey(opRef)) {
                throw new IllegalStateException("operationRef '" + opRef + "' not found");
              }
            }
          }
          setMcpServerStatus(server, "Ready");
        } catch (Exception e) {
          String msg = "Invalid: " + e.getMessage();
          setMcpServerStatus(server, msg);
          errors.put("McpServer/" + server.getMetadata().getName(), msg);
        }
      }
    }

    if (!errors.isEmpty()) {
      StringBuilder sb = new StringBuilder("Schema has invalid resources:");
      for (Map.Entry<String, String> entry : errors.entrySet()) {
        sb.append(" ").append(entry.getKey()).append("=").append(entry.getValue()).append(";");
      }
      throw new IllegalStateException(sb.toString());
    }
  }

  private void validateQueryResource(graphql.schema.idl.TypeDefinitionRegistry registry,
                                     Query query,
                                     Set<String> pythonFunctions) {
    QuerySpec spec = query.getSpec();
    if (spec == null) {
      throw new IllegalStateException("spec is required");
    }
    String typeName = (spec.getTypeName() == null || spec.getTypeName().isBlank()) ? "Query" : spec.getTypeName();
    String fieldName = (spec.getFieldName() == null || spec.getFieldName().isBlank())
      ? query.getMetadata().getName()
      : spec.getFieldName();
    graphql.language.TypeDefinition<?> typeDef = registry.getType(typeName).orElse(null);
    if (!(typeDef instanceof graphql.language.ObjectTypeDefinition)) {
      throw new IllegalStateException("type '" + typeName + "' not found in schema");
    }
    graphql.language.ObjectTypeDefinition obj = (graphql.language.ObjectTypeDefinition) typeDef;
    graphql.language.FieldDefinition fieldDef = obj.getFieldDefinitions().stream()
      .filter(f -> fieldName.equals(f.getName()))
      .findFirst()
      .orElse(null);
    if (fieldDef == null) {
      throw new IllegalStateException("field '" + typeName + "." + fieldName + "' not found in schema");
    }
    // Only validate $N placeholders for engines that use sql.
    if (spec.getEngine() == null
      || spec.getEngine() == QuerySpec.Engine.POSTGRES
      || spec.getEngine() == QuerySpec.Engine.DUCKDB
      || spec.getEngine() == QuerySpec.Engine.JDBC) {
      validateSqlParams(spec.getSql(), spec.getParams());
    }
    validateParamSourcesAgainstArgs(fieldDef, spec.getParams(), pythonFunctions);
  }

  private void validatePostgresMutationResource(graphql.schema.idl.TypeDefinitionRegistry registry,
                                                PostgresMutation mutation,
                                                Set<String> pythonFunctions) {
    PostgresMutationSpec spec = mutation.getSpec();
    if (spec == null) {
      throw new IllegalStateException("spec is required");
    }
    String mutationName = (spec.getMutationName() == null || spec.getMutationName().isBlank())
      ? mutation.getMetadata().getName()
      : spec.getMutationName();
    graphql.language.TypeDefinition<?> typeDef = registry.getType("Mutation").orElse(null);
    if (!(typeDef instanceof graphql.language.ObjectTypeDefinition)) {
      throw new IllegalStateException("type 'Mutation' not found in schema");
    }
    graphql.language.ObjectTypeDefinition obj = (graphql.language.ObjectTypeDefinition) typeDef;
    graphql.language.FieldDefinition fieldDef = obj.getFieldDefinitions().stream()
      .filter(f -> mutationName.equals(f.getName()))
      .findFirst()
      .orElse(null);
    if (fieldDef == null) {
      throw new IllegalStateException("field 'Mutation." + mutationName + "' not found in schema");
    }
    validateSqlParams(spec.getSql(), spec.getParams());
    validateParamSourcesAgainstArgs(fieldDef, spec.getParams(), pythonFunctions);
  }

  private void validateSqlParams(String sql, List<QueryParam> params) {
    if (sql == null || sql.isBlank()) {
      throw new IllegalStateException("spec.sql is required");
    }
    int max = 0;
    Matcher m = SQL_PARAM.matcher(sql);
    while (m.find()) {
      int idx = Integer.parseInt(m.group(1));
      if (idx > max) max = idx;
    }
    if (max == 0) {
      return;
    }
    Map<Integer, QueryParam> index = new HashMap<>();
    if (params != null) {
      for (QueryParam p : params) {
        if (p == null) continue;
        if (index.containsKey(p.getIndex())) {
          throw new IllegalStateException("duplicate param index $" + p.getIndex());
        }
        index.put(p.getIndex(), p);
      }
    }
    for (int i = 1; i <= max; i++) {
      if (!index.containsKey(i)) {
        throw new IllegalStateException("missing param mapping for $" + i);
      }
    }
  }

  private void validateParamSourcesAgainstArgs(graphql.language.FieldDefinition fieldDef,
                                               List<QueryParam> params,
                                               Set<String> pythonFunctions) {
    if (params == null || params.isEmpty()) {
      return;
    }
    Set<String> argNames = new HashSet<>();
    if (fieldDef.getInputValueDefinitions() != null) {
      for (graphql.language.InputValueDefinition arg : fieldDef.getInputValueDefinitions()) {
        if (arg != null && arg.getName() != null && !arg.getName().isBlank()) {
          argNames.add(arg.getName());
        }
      }
    }

    for (QueryParam param : params) {
      if (param == null || param.getSource() == null || param.getSource().getKind() == null) {
        continue;
      }
      ParamSource src = param.getSource();
      if (src.getKind() == ParamSource.Kind.ARG) {
        String name = src.getName();
        if (name == null || name.isBlank()) {
          throw new IllegalStateException("ARG param missing name for $" + param.getIndex());
        }
        if (name.startsWith("$")) {
          continue; // jsonpath; can't validate safely here
        }
        String root = name;
        int dot = root.indexOf('.');
        if (dot >= 0) {
          root = root.substring(0, dot);
        }
        int bracket = root.indexOf('[');
        if (bracket >= 0) {
          root = root.substring(0, bracket);
        }
        if (!argNames.isEmpty() && !argNames.contains(root)) {
          throw new IllegalStateException("ARG '" + root + "' not found in field args (param $" + param.getIndex() + ")");
        }
      } else if (src.getKind() == ParamSource.Kind.PYTHON) {
        String fnRef = src.getName();
        if (src.getPython() != null && src.getPython().getFunctionRef() != null && !src.getPython().getFunctionRef().isBlank()) {
          fnRef = src.getPython().getFunctionRef();
        }
        if (fnRef == null || fnRef.isBlank()) {
          throw new IllegalStateException("PYTHON param missing functionRef (param $" + param.getIndex() + ")");
        }
        if (pythonFunctions != null && !pythonFunctions.contains(fnRef)) {
          throw new IllegalStateException("PYTHON function '" + fnRef + "' not found (param $" + param.getIndex() + ")");
        }
      }
    }
  }

}
