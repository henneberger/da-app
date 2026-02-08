package dev.henneberger.runtime.config;

import java.util.List;

public class McpServerConfig {
  private String name;
  private String path;
  private Boolean requireAuth;
  private List<String> operationRefs;
  private ToolNameStrategy toolNameStrategy;

  public enum ToolNameStrategy {
    RESOURCE_NAME,
    OPERATION_NAME
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

