package dev.henneberger.runtime.config;

public class ElasticsearchConnectionConfig {
  private String name;
  private String endpoint;
  private String username;
  private String password;
  private String passwordEnv;
  private String apiKey;
  private String apiKeyEnv;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
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

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public String getApiKeyEnv() {
    return apiKeyEnv;
  }

  public void setApiKeyEnv(String apiKeyEnv) {
    this.apiKeyEnv = apiKeyEnv;
  }
}
