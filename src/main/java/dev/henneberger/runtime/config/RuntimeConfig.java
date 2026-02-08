package dev.henneberger.runtime.config;

import java.util.List;

public class RuntimeConfig {

  private List<ConnectionConfig> connections;
  private List<DuckDbConnectionConfig> duckdbConnections;
  private String schema;
  private List<QueryConfig> queries;
  private List<PythonFunctionConfig> pythonFunctions;
  private List<KafkaConnectionConfig> kafkaConnections;
  private List<KafkaEventConfig> kafkaEvents;
  private List<PostgresMutationConfig> postgresMutations;
  private List<PostgresSubscriptionConfig> postgresSubscriptions;
  private List<KafkaSubscriptionConfig> kafkaSubscriptions;
  private List<IngestionEndpointConfig> ingestionEndpoints;
  private List<JsonSchemaConfig> jsonSchemas;
  private List<GraphQLOperationConfig> graphQLOperations;
  private List<RestGraphQLEndpointConfig> restGraphQLEndpoints;
  private List<McpServerConfig> mcpServers;
  private AuthConfig auth;
  private ObservabilityConfig observability;
  private List<MongoConnectionConfig> mongoConnections;
  private List<ElasticsearchConnectionConfig> elasticsearchConnections;
  private List<JdbcConnectionConfig> jdbcConnections;
  private List<DynamoDbConnectionConfig> dynamodbConnections;

  public List<ConnectionConfig> getConnections() {
    return connections;
  }

  public void setConnections(List<ConnectionConfig> connections) {
    this.connections = connections;
  }

  public List<DuckDbConnectionConfig> getDuckdbConnections() {
    return duckdbConnections;
  }

  public void setDuckdbConnections(List<DuckDbConnectionConfig> duckdbConnections) {
    this.duckdbConnections = duckdbConnections;
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public List<QueryConfig> getQueries() {
    return queries;
  }

  public void setQueries(List<QueryConfig> queries) {
    this.queries = queries;
  }

  public List<PythonFunctionConfig> getPythonFunctions() {
    return pythonFunctions;
  }

  public void setPythonFunctions(List<PythonFunctionConfig> pythonFunctions) {
    this.pythonFunctions = pythonFunctions;
  }

  public List<KafkaConnectionConfig> getKafkaConnections() {
    return kafkaConnections;
  }

  public void setKafkaConnections(List<KafkaConnectionConfig> kafkaConnections) {
    this.kafkaConnections = kafkaConnections;
  }

  public List<KafkaEventConfig> getKafkaEvents() {
    return kafkaEvents;
  }

  public void setKafkaEvents(List<KafkaEventConfig> kafkaEvents) {
    this.kafkaEvents = kafkaEvents;
  }

  public List<PostgresMutationConfig> getPostgresMutations() {
    return postgresMutations;
  }

  public void setPostgresMutations(List<PostgresMutationConfig> postgresMutations) {
    this.postgresMutations = postgresMutations;
  }

  public List<PostgresSubscriptionConfig> getPostgresSubscriptions() {
    return postgresSubscriptions;
  }

  public void setPostgresSubscriptions(List<PostgresSubscriptionConfig> postgresSubscriptions) {
    this.postgresSubscriptions = postgresSubscriptions;
  }

  public List<KafkaSubscriptionConfig> getKafkaSubscriptions() {
    return kafkaSubscriptions;
  }

  public void setKafkaSubscriptions(List<KafkaSubscriptionConfig> kafkaSubscriptions) {
    this.kafkaSubscriptions = kafkaSubscriptions;
  }

  public List<IngestionEndpointConfig> getIngestionEndpoints() {
    return ingestionEndpoints;
  }

  public void setIngestionEndpoints(List<IngestionEndpointConfig> ingestionEndpoints) {
    this.ingestionEndpoints = ingestionEndpoints;
  }

  public List<JsonSchemaConfig> getJsonSchemas() {
    return jsonSchemas;
  }

  public void setJsonSchemas(List<JsonSchemaConfig> jsonSchemas) {
    this.jsonSchemas = jsonSchemas;
  }

  public List<GraphQLOperationConfig> getGraphQLOperations() {
    return graphQLOperations;
  }

  public void setGraphQLOperations(List<GraphQLOperationConfig> graphQLOperations) {
    this.graphQLOperations = graphQLOperations;
  }

  public List<RestGraphQLEndpointConfig> getRestGraphQLEndpoints() {
    return restGraphQLEndpoints;
  }

  public void setRestGraphQLEndpoints(List<RestGraphQLEndpointConfig> restGraphQLEndpoints) {
    this.restGraphQLEndpoints = restGraphQLEndpoints;
  }

  public List<McpServerConfig> getMcpServers() {
    return mcpServers;
  }

  public void setMcpServers(List<McpServerConfig> mcpServers) {
    this.mcpServers = mcpServers;
  }

  public AuthConfig getAuth() {
    return auth;
  }

  public void setAuth(AuthConfig auth) {
    this.auth = auth;
  }

  public List<MongoConnectionConfig> getMongoConnections() {
    return mongoConnections;
  }

  public void setMongoConnections(List<MongoConnectionConfig> mongoConnections) {
    this.mongoConnections = mongoConnections;
  }

  public List<ElasticsearchConnectionConfig> getElasticsearchConnections() {
    return elasticsearchConnections;
  }

  public void setElasticsearchConnections(List<ElasticsearchConnectionConfig> elasticsearchConnections) {
    this.elasticsearchConnections = elasticsearchConnections;
  }

  public List<JdbcConnectionConfig> getJdbcConnections() {
    return jdbcConnections;
  }

  public void setJdbcConnections(List<JdbcConnectionConfig> jdbcConnections) {
    this.jdbcConnections = jdbcConnections;
  }

  public List<DynamoDbConnectionConfig> getDynamodbConnections() {
    return dynamodbConnections;
  }

  public void setDynamodbConnections(List<DynamoDbConnectionConfig> dynamodbConnections) {
    this.dynamodbConnections = dynamodbConnections;
  }

  public ObservabilityConfig getObservability() {
    return observability;
  }

  public void setObservability(ObservabilityConfig observability) {
    this.observability = observability;
  }
}
