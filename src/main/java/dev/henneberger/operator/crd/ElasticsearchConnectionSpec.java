package dev.henneberger.operator.crd;

public class ElasticsearchConnectionSpec {
  private String endpoint;
  private String username;
  private String password;
  private SecretKeyRef passwordSecretRef;
  private String apiKey;
  private SecretKeyRef apiKeySecretRef;

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

  public SecretKeyRef getPasswordSecretRef() {
    return passwordSecretRef;
  }

  public void setPasswordSecretRef(SecretKeyRef passwordSecretRef) {
    this.passwordSecretRef = passwordSecretRef;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  public SecretKeyRef getApiKeySecretRef() {
    return apiKeySecretRef;
  }

  public void setApiKeySecretRef(SecretKeyRef apiKeySecretRef) {
    this.apiKeySecretRef = apiKeySecretRef;
  }
}
