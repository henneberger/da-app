package dev.henneberger.operator.crd;

import java.util.List;

public class RestGraphQLEndpointSpec {
  private List<String> schemaRefs;
  private String path;
  private String method;
  private String contentType;
  private Integer maxBodyBytes;
  private Boolean requireJson;
  private Boolean requireAuth;

  private String operationRef;
  private String document;
  private String operationName;
  private List<RestVariable> variables;

  public static class RestVariable {
    private String name;
    private RestParamSource source;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public RestParamSource getSource() {
      return source;
    }

    public void setSource(RestParamSource source) {
      this.source = source;
    }
  }

  public static class RestParamSource {
    public enum Kind {
      BODY,
      QUERY,
      PATH,
      HEADER,
      JWT,
      ENV,
      RAW_BODY
    }

    private Kind kind;
    private String name;

    public Kind getKind() {
      return kind;
    }

    public void setKind(Kind kind) {
      this.kind = kind;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }
  }

  public List<String> getSchemaRefs() {
    return schemaRefs;
  }

  public void setSchemaRefs(List<String> schemaRefs) {
    this.schemaRefs = schemaRefs;
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

  public List<RestVariable> getVariables() {
    return variables;
  }

  public void setVariables(List<RestVariable> variables) {
    this.variables = variables;
  }
}

