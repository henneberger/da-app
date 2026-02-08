package dev.henneberger.operator.crd;

import java.util.List;

public class PostgresMutationSpec {

  private String connectionRef;
  private List<String> schemaRefs;
  private String mutationName;
  private String sql;
  private String description;
  private List<QueryParam> params;
  private List<EnrichField> enrich;
  private Integer timeoutMs;
  private List<String> requiredRoles;
  private List<String> requiredScopes;

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

  public String getMutationName() {
    return mutationName;
  }

  public void setMutationName(String mutationName) {
    this.mutationName = mutationName;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
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

  public List<EnrichField> getEnrich() {
    return enrich;
  }

  public void setEnrich(List<EnrichField> enrich) {
    this.enrich = enrich;
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
}
