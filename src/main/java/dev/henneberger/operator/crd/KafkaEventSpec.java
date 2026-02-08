package dev.henneberger.operator.crd;

public class KafkaEventSpec {

  private String connectionRef;
  private java.util.List<String> schemaRefs;
  private String eventName;
  private String topic;
  private String partitionKeyField;
  private java.util.List<String> partitionKeyFields;
  private String description;
  private java.util.List<EnrichField> enrich;
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

  public String getEventName() {
    return eventName;
  }

  public void setEventName(String eventName) {
    this.eventName = eventName;
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

  public java.util.List<EnrichField> getEnrich() {
    return enrich;
  }

  public void setEnrich(java.util.List<EnrichField> enrich) {
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
