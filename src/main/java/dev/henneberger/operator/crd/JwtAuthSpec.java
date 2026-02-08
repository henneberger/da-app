package dev.henneberger.operator.crd;

import java.util.List;

public class JwtAuthSpec {

  private String jwksUrl;
  private String issuer;
  private List<String> audiences;
  private String sharedSecret;
  private String sharedSecretEnv;
  private Long jwksCacheSeconds;
  private Long clockSkewSeconds;

  public String getJwksUrl() {
    return jwksUrl;
  }

  public void setJwksUrl(String jwksUrl) {
    this.jwksUrl = jwksUrl;
  }

  public String getIssuer() {
    return issuer;
  }

  public void setIssuer(String issuer) {
    this.issuer = issuer;
  }

  public List<String> getAudiences() {
    return audiences;
  }

  public void setAudiences(List<String> audiences) {
    this.audiences = audiences;
  }

  public String getSharedSecret() {
    return sharedSecret;
  }

  public void setSharedSecret(String sharedSecret) {
    this.sharedSecret = sharedSecret;
  }

  public String getSharedSecretEnv() {
    return sharedSecretEnv;
  }

  public void setSharedSecretEnv(String sharedSecretEnv) {
    this.sharedSecretEnv = sharedSecretEnv;
  }

  public Long getJwksCacheSeconds() {
    return jwksCacheSeconds;
  }

  public void setJwksCacheSeconds(Long jwksCacheSeconds) {
    this.jwksCacheSeconds = jwksCacheSeconds;
  }

  public Long getClockSkewSeconds() {
    return clockSkewSeconds;
  }

  public void setClockSkewSeconds(Long clockSkewSeconds) {
    this.clockSkewSeconds = clockSkewSeconds;
  }
}
