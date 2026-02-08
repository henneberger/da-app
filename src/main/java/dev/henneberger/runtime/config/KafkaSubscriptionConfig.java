package dev.henneberger.runtime.config;

public class KafkaSubscriptionConfig {
  private String name;
  private String connectionRef;
  private String fieldName;
  private String topic;
  private String format;
  private java.util.List<KafkaSubscriptionFilterConfig> filters;
  private java.util.List<String> requiredRoles;
  private java.util.List<String> requiredScopes;

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

  public java.util.List<KafkaSubscriptionFilterConfig> getFilters() {
    return filters;
  }

  public void setFilters(java.util.List<KafkaSubscriptionFilterConfig> filters) {
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
