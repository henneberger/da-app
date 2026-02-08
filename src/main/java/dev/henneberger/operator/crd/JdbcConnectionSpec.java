package dev.henneberger.operator.crd;

public class JdbcConnectionSpec {
  private String jdbcUrl;
  private String driverClass;
  private String user;
  private String password;
  private SecretKeyRef passwordSecretRef;

  public String getJdbcUrl() {
    return jdbcUrl;
  }

  public void setJdbcUrl(String jdbcUrl) {
    this.jdbcUrl = jdbcUrl;
  }

  public String getDriverClass() {
    return driverClass;
  }

  public void setDriverClass(String driverClass) {
    this.driverClass = driverClass;
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
}
