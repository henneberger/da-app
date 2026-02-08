package dev.henneberger.operator.crd;

import java.util.Map;

public class MongoConnectionSpec {
  private String host;
  private Integer port = 27017;
  private String database;
  private String user;
  private String password;
  private SecretKeyRef passwordSecretRef;
  private String authSource;
  private Boolean ssl = false;
  private Map<String, String> options;

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

  public SecretKeyRef getPasswordSecretRef() {
    return passwordSecretRef;
  }

  public void setPasswordSecretRef(SecretKeyRef passwordSecretRef) {
    this.passwordSecretRef = passwordSecretRef;
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
