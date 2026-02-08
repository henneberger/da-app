package dev.henneberger.runtime.config;

import java.util.List;

public class QueryConfig {

  public enum ResultMode {
    LIST,
    SINGLE
  }

  public enum Engine {
    POSTGRES,
    DUCKDB,
    JDBC,
    MONGODB,
    ELASTICSEARCH,
    DYNAMODB
  }

  private String name;
  private String fieldName;
  private String connectionRef;
  private String typeName;
  private String sql;
  private ResultMode resultMode;
  private Engine engine = Engine.POSTGRES;
  private String description;
  private List<QueryParamConfig> params;
  private Integer timeoutMs;
  private List<String> requiredRoles;
  private List<String> requiredScopes;
  private PaginationConfig pagination;
  private MongoQueryConfig mongo;
  private ElasticsearchQueryConfig elasticsearch;
  private DynamoDbQueryConfig dynamodb;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getConnectionRef() {
    return connectionRef;
  }

  public void setConnectionRef(String connectionRef) {
    this.connectionRef = connectionRef;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public ResultMode getResultMode() {
    return resultMode;
  }

  public void setResultMode(ResultMode resultMode) {
    this.resultMode = resultMode;
  }

  public Engine getEngine() {
    return engine == null ? Engine.POSTGRES : engine;
  }

  public void setEngine(Engine engine) {
    this.engine = engine;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public List<QueryParamConfig> getParams() {
    return params;
  }

  public void setParams(List<QueryParamConfig> params) {
    this.params = params;
  }

  public Integer getTimeoutMs() {
    return timeoutMs;
  }

  public void setTimeoutMs(Integer timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  public List<String> getRequiredRoles() {
    return requiredRoles;
  }

  public void setRequiredRoles(List<String> requiredRoles) {
    this.requiredRoles = requiredRoles;
  }

  public List<String> getRequiredScopes() {
    return requiredScopes;
  }

  public void setRequiredScopes(List<String> requiredScopes) {
    this.requiredScopes = requiredScopes;
  }

  public PaginationConfig getPagination() {
    return pagination;
  }

  public void setPagination(PaginationConfig pagination) {
    this.pagination = pagination;
  }

  public MongoQueryConfig getMongo() {
    return mongo;
  }

  public void setMongo(MongoQueryConfig mongo) {
    this.mongo = mongo;
  }

  public ElasticsearchQueryConfig getElasticsearch() {
    return elasticsearch;
  }

  public void setElasticsearch(ElasticsearchQueryConfig elasticsearch) {
    this.elasticsearch = elasticsearch;
  }

  public DynamoDbQueryConfig getDynamodb() {
    return dynamodb;
  }

  public void setDynamodb(DynamoDbQueryConfig dynamodb) {
    this.dynamodb = dynamodb;
  }
}
