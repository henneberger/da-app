package dev.henneberger.runtime.config;

public class DynamoDbConnectionConfig {
  private String name;
  private String region;
  private String endpoint;
  private String accessKey;
  private String secretKey;
  private String accessKeyEnv;
  private String secretKeyEnv;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(String secretKey) {
    this.secretKey = secretKey;
  }

  public String getAccessKeyEnv() {
    return accessKeyEnv;
  }

  public void setAccessKeyEnv(String accessKeyEnv) {
    this.accessKeyEnv = accessKeyEnv;
  }

  public String getSecretKeyEnv() {
    return secretKeyEnv;
  }

  public void setSecretKeyEnv(String secretKeyEnv) {
    this.secretKeyEnv = secretKeyEnv;
  }
}
