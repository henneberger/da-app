package dev.henneberger.operator.crd;

public class CursorSigningSpec {

  public enum Method {
    HMAC,
    JWT
  }

  private Method method = Method.HMAC;
  private String sharedSecret;
  private String sharedSecretEnv;
  private Boolean useAuthJwtSecret;

  public Method getMethod() {
    return method == null ? Method.HMAC : method;
  }

  public void setMethod(Method method) {
    this.method = method;
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

  public Boolean getUseAuthJwtSecret() {
    return useAuthJwtSecret;
  }

  public void setUseAuthJwtSecret(Boolean useAuthJwtSecret) {
    this.useAuthJwtSecret = useAuthJwtSecret;
  }
}
