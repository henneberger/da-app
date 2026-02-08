package dev.henneberger.operator.crd;

import java.util.Map;

public class DuckDBConnectionSpec {

  private String database;
  private Map<String, String> settings;

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public Map<String, String> getSettings() {
    return settings;
  }

  public void setSettings(Map<String, String> settings) {
    this.settings = settings;
  }
}
