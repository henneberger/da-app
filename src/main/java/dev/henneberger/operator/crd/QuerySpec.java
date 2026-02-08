package dev.henneberger.operator.crd;

import java.util.List;

public class QuerySpec {

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

  private String connectionRef;
  private List<String> schemaRefs;
  private String typeName;
  private String fieldName;
  private String sql;
  private ResultMode resultMode = ResultMode.LIST;
  private Engine engine = Engine.POSTGRES;
  private String description;
  private List<QueryParam> params;
  private Integer timeoutMs;
  private List<String> requiredRoles;
  private List<String> requiredScopes;
  private PaginationSpec pagination;
  private MongoQuerySpec mongo;
  private ElasticsearchQuerySpec elasticsearch;
  private DynamoDbQuerySpec dynamodb;

  public String getConnectionRef() {
    return connectionRef;
  }

  public void setConnectionRef(String connectionRef) {
    this.connectionRef = connectionRef;
  }

  public List<String> getSchemaRefs() {
    return schemaRefs;
  }

  public void setSchemaRefs(List<String> schemaRefs) {
    this.schemaRefs = schemaRefs;
  }

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
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

  public List<QueryParam> getParams() {
    return params;
  }

  public void setParams(List<QueryParam> params) {
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

  public PaginationSpec getPagination() {
    return pagination;
  }

  public void setPagination(PaginationSpec pagination) {
    this.pagination = pagination;
  }

  public MongoQuerySpec getMongo() {
    return mongo;
  }

  public void setMongo(MongoQuerySpec mongo) {
    this.mongo = mongo;
  }

  public ElasticsearchQuerySpec getElasticsearch() {
    return elasticsearch;
  }

  public void setElasticsearch(ElasticsearchQuerySpec elasticsearch) {
    this.elasticsearch = elasticsearch;
  }

  public DynamoDbQuerySpec getDynamodb() {
    return dynamodb;
  }

  public void setDynamodb(DynamoDbQuerySpec dynamodb) {
    this.dynamodb = dynamodb;
  }
}
