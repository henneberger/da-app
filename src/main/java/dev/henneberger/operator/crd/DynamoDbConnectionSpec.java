package dev.henneberger.operator.crd;

public class DynamoDbConnectionSpec {
  private String region;
  private String endpoint;
  private String accessKey;
  private String secretKey;
  private SecretKeyRef accessKeySecretRef;
  private SecretKeyRef secretKeySecretRef;

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

  public SecretKeyRef getAccessKeySecretRef() {
    return accessKeySecretRef;
  }

  public void setAccessKeySecretRef(SecretKeyRef accessKeySecretRef) {
    this.accessKeySecretRef = accessKeySecretRef;
  }

  public SecretKeyRef getSecretKeySecretRef() {
    return secretKeySecretRef;
  }

  public void setSecretKeySecretRef(SecretKeyRef secretKeySecretRef) {
    this.secretKeySecretRef = secretKeySecretRef;
  }
}
