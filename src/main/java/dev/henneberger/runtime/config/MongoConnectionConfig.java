package dev.henneberger.runtime.config;

import java.util.Map;

public class MongoConnectionConfig {
  private String name;
  private String host;
  private Integer port;
  private String database;
  private String user;
  private String password;
  private String passwordEnv;
  private String authSource;
  private Boolean ssl;
  private Map<String, String> options;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public void setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
  }

  public String getAuthSource() {
    return authSource;
  }

  public void setAuthSource(String authSource) {
    this.authSource = authSource;
  }

  public Boolean getSsl() {
    return ssl;
  }

  public void setSsl(Boolean ssl) {
    this.ssl = ssl;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public void setOptions(Map<String, String> options) {
    this.options = options;
  }
}
