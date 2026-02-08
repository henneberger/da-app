package dev.henneberger.runtime.config;

import java.util.List;

public class IngestionEndpointConfig {

  private String name;
  private String connectionRef;
  private String path;
  private String method;
  private String contentType;
  private Integer maxBodyBytes;
  private Boolean requireJson;
  private String topic;
  private String partitionKeyField;
  private String jsonSchemaRef;
  private List<AddFieldConfig> addFields;
  private Integer responseCode;
  private String responseContentType;
  private List<String> responseFields;

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

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getMethod() {
    return method;
  }

  public void setMethod(String method) {
    this.method = method;
  }

  public String getContentType() {
    return contentType;
  }

  public void setContentType(String contentType) {
    this.contentType = contentType;
  }

  public Integer getMaxBodyBytes() {
    return maxBodyBytes;
  }

  public void setMaxBodyBytes(Integer maxBodyBytes) {
    this.maxBodyBytes = maxBodyBytes;
  }

  public Boolean getRequireJson() {
    return requireJson;
  }

  public void setRequireJson(Boolean requireJson) {
    this.requireJson = requireJson;
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

  public String getJsonSchemaRef() {
    return jsonSchemaRef;
  }

  public void setJsonSchemaRef(String jsonSchemaRef) {
    this.jsonSchemaRef = jsonSchemaRef;
  }

  public List<AddFieldConfig> getAddFields() {
    return addFields;
  }

  public void setAddFields(List<AddFieldConfig> addFields) {
    this.addFields = addFields;
  }

  public Integer getResponseCode() {
    return responseCode;
  }

  public void setResponseCode(Integer responseCode) {
    this.responseCode = responseCode;
  }

  public String getResponseContentType() {
    return responseContentType;
  }

  public void setResponseContentType(String responseContentType) {
    this.responseContentType = responseContentType;
  }

  public List<String> getResponseFields() {
    return responseFields;
  }

  public void setResponseFields(List<String> responseFields) {
    this.responseFields = responseFields;
  }
}
