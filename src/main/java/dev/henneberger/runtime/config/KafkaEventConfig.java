package dev.henneberger.runtime.config;

public class KafkaEventConfig {

  private String name;
  private String connectionRef;
  private String topic;
  private String partitionKeyField;
  private java.util.List<String> partitionKeyFields;
  private String description;
  private java.util.List<EnrichFieldConfig> enrich;
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

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getPartitionKeyField() {
    return partitionKeyField;
  }

  public void setPartitionKeyField(String partitionKeyField) {
    this.partitionKeyField = partitionKeyField;
  }

  public java.util.List<String> getPartitionKeyFields() {
    return partitionKeyFields;
  }

  public void setPartitionKeyFields(java.util.List<String> partitionKeyFields) {
    this.partitionKeyFields = partitionKeyFields;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public java.util.List<EnrichFieldConfig> getEnrich() {
    return enrich;
  }

  public void setEnrich(java.util.List<EnrichFieldConfig> enrich) {
    this.enrich = enrich;
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
