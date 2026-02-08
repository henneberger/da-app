package dev.henneberger.runtime.config;

import java.util.List;

public class PostgresMutationConfig {

  private String name;
  private String connectionRef;
  private String sql;
  private String description;
  private List<QueryParamConfig> params;
  private List<EnrichFieldConfig> enrich;
  private Integer timeoutMs;
  private List<String> requiredRoles;
  private List<String> requiredScopes;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getConnectionRef() {
    return connectionRef;
  }

  public void setConnectionRef(String connectionRef) {
    this.connectionRef = connectionRef;
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

  public List<QueryParamConfig> getParams() {
    return params;
  }

  public void setParams(List<QueryParamConfig> params) {
    this.params = params;
  }

  public List<EnrichFieldConfig> getEnrich() {
    return enrich;
  }

  public void setEnrich(List<EnrichFieldConfig> enrich) {
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
