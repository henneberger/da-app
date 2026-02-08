package dev.henneberger.operator.crd;

public class KafkaSubscriptionSpec {
  private String connectionRef;
  private java.util.List<String> schemaRefs;
  private String fieldName;
  private String topic;
  private String format;
  private java.util.List<KafkaSubscriptionFilter> filters;
  private java.util.List<String> requiredRoles;
  private java.util.List<String> requiredScopes;

  public String getConnectionRef() {
    return connectionRef;
  }

  public void setConnectionRef(String connectionRef) {
    this.connectionRef = connectionRef;
  }

  public java.util.List<String> getSchemaRefs() {
    return schemaRefs;
  }

  public void setSchemaRefs(java.util.List<String> schemaRefs) {
    this.schemaRefs = schemaRefs;
  }

  public String getFieldName() {
    return fieldName;
  }

  public void setFieldName(String fieldName) {
    this.fieldName = fieldName;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getFormat() {
    return format;
  }

  public void setFormat(String format) {
    this.format = format;
  }

  public java.util.List<KafkaSubscriptionFilter> getFilters() {
    return filters;
  }

  public void setFilters(java.util.List<KafkaSubscriptionFilter> filters) {
    this.filters = filters;
  }

  public java.util.List<String> getRequiredRoles() {
    return requiredRoles;
  }

  public void setRequiredRoles(java.util.List<String> requiredRoles) {
    this.requiredRoles = requiredRoles;
  }

  public java.util.List<String> getRequiredScopes() {
    return requiredScopes;
  }

  public void setRequiredScopes(java.util.List<String> requiredScopes) {
    this.requiredScopes = requiredScopes;
  }
}
