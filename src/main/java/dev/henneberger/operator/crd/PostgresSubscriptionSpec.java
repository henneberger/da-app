package dev.henneberger.operator.crd;

import java.util.List;

public class PostgresSubscriptionSpec {

  public enum Operation {
    INSERT,
    UPDATE,
    DELETE
  }

  private String connectionRef;
  private List<String> schemaRefs;
  private String fieldName;
  private String table;
  private List<Operation> operations;
  private List<PostgresSubscriptionFilter> filters;
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

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
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

  public List<PostgresSubscriptionFilter> getFilters() {
    return filters;
  }

  public void setFilters(List<PostgresSubscriptionFilter> filters) {
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
