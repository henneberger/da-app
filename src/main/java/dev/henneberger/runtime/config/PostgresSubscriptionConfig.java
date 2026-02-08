package dev.henneberger.runtime.config;

import java.util.List;

public class PostgresSubscriptionConfig {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private String name;
  private String fieldName;
  private String connectionRef;
  private String table;
  private List<Operation> operations;
  private List<PostgresSubscriptionFilterConfig> filters;
  private List<String> requiredRoles;
  private List<String> requiredScopes;

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

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public void setOperations(List<Operation> operations) {
    this.operations = operations;
  }

  public List<PostgresSubscriptionFilterConfig> getFilters() {
    return filters;
  }

  public void setFilters(List<PostgresSubscriptionFilterConfig> filters) {
    this.filters = filters;
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
