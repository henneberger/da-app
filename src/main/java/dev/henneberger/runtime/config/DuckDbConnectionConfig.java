package dev.henneberger.runtime.config;

import java.util.Map;

public class DuckDbConnectionConfig {

  private String name;
  private String database;
  private Map<String, String> settings;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

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
