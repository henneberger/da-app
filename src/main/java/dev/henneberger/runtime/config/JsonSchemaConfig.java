package dev.henneberger.runtime.config;

import java.util.Map;

public class JsonSchemaConfig {

  private String name;
  private String description;
  private Map<String, Object> schema;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public Map<String, Object> getSchema() {
    return schema;
  }

  public void setSchema(Map<String, Object> schema) {
    this.schema = schema;
  }
}
