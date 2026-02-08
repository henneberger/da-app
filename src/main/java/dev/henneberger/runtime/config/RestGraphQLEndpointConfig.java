package dev.henneberger.runtime.config;

import java.util.List;

public class RestGraphQLEndpointConfig {
  private String name;
  private String path;
  private String method;
  private String contentType;
  private Integer maxBodyBytes;
  private Boolean requireJson;
  private Boolean requireAuth;
  private String operationRef;
  private String document;
  private String operationName;
  private List<Variable> variables;

  public static class Variable {
    private String name;
    private RestParamSourceConfig source;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public RestParamSourceConfig getSource() {
      return source;
    }

    public void setSource(RestParamSourceConfig source) {
      this.source = source;
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
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

  public Boolean getRequireAuth() {
    return requireAuth;
  }

  public void setRequireAuth(Boolean requireAuth) {
    this.requireAuth = requireAuth;
  }

  public String getOperationRef() {
    return operationRef;
  }

  public void setOperationRef(String operationRef) {
    this.operationRef = operationRef;
  }

  public String getDocument() {
    return document;
  }

  public void setDocument(String document) {
    this.document = document;
  }

  public String getOperationName() {
    return operationName;
  }

  public void setOperationName(String operationName) {
    this.operationName = operationName;
  }

  public List<Variable> getVariables() {
    return variables;
  }

  public void setVariables(List<Variable> variables) {
    this.variables = variables;
  }
}

