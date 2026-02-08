package dev.henneberger.operator.crd;

import java.util.Map;

public class JsonSchemaSpec {

  private String description;
  private Map<String, Object> schema;

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
