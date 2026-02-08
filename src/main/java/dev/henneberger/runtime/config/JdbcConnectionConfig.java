package dev.henneberger.runtime.config;

public class JdbcConnectionConfig {
  private String name;
  private String jdbcUrl;
  private String driverClass;
  private String user;
  private String password;
  private String passwordEnv;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

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

  public String getPasswordEnv() {
    return passwordEnv;
  }

  public void setPasswordEnv(String passwordEnv) {
    this.passwordEnv = passwordEnv;
  }
}
