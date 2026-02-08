package dev.henneberger.runtime.config;

public class HealthCheckConfig {

  public enum Type {
    JDBC,
    DUCKDB,
    KAFKA
  }

  private String name;
  private Type type;
  private String connectionRef;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public String getConnectionRef() {
    return connectionRef;
  }

  public void setConnectionRef(String connectionRef) {
    this.connectionRef = connectionRef;
  }
}
