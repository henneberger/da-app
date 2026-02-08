package dev.henneberger.operator.crd;

import java.util.List;

public class McpServerSpec {
  private List<String> schemaRefs;

  // HTTP path to expose the MCP server on (streamable HTTP style).
  private String path;

  // When auth is configured on the runtime, require auth for MCP calls.
  private Boolean requireAuth;

  // Optional: only expose a subset of GraphQLOperations by name.
  // When null/empty, all GraphQLOperations matching schemaRefs are exposed as tools.
  private List<String> operationRefs;

  // How to name tools derived from GraphQLOperations.
  private ToolNameStrategy toolNameStrategy;

  public enum ToolNameStrategy {
    RESOURCE_NAME,
    OPERATION_NAME
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

  public Boolean getRequireAuth() {
    return requireAuth;
  }

  public void setRequireAuth(Boolean requireAuth) {
    this.requireAuth = requireAuth;
  }

  public List<String> getOperationRefs() {
    return operationRefs;
  }

  public void setOperationRefs(List<String> operationRefs) {
    this.operationRefs = operationRefs;
  }

  public ToolNameStrategy getToolNameStrategy() {
    return toolNameStrategy;
  }

  public void setToolNameStrategy(ToolNameStrategy toolNameStrategy) {
    this.toolNameStrategy = toolNameStrategy;
  }
}

