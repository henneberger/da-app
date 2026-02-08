package dev.henneberger.runtime.config;

public class ConnectionConfig {

  private String name;
  private String host;
  private Integer port;
  private String database;
  private String user;
  private Boolean ssl;
  private PoolConfig pool;
  private String password;
  private String passwordEnv;

  public static class PoolConfig {
    private Integer maxSize;
    private Integer maxWaitQueueSize;
    private Integer idleTimeoutSeconds;
    private Integer connectionTimeoutSeconds;

    public Integer getMaxSize() {
      return maxSize;
    }

    public void setMaxSize(Integer maxSize) {
      this.maxSize = maxSize;
    }

    public Integer getMaxWaitQueueSize() {
      return maxWaitQueueSize;
    }

    public void setMaxWaitQueueSize(Integer maxWaitQueueSize) {
      this.maxWaitQueueSize = maxWaitQueueSize;
    }

    public Integer getIdleTimeoutSeconds() {
      return idleTimeoutSeconds;
    }

    public void setIdleTimeoutSeconds(Integer idleTimeoutSeconds) {
      this.idleTimeoutSeconds = idleTimeoutSeconds;
    }

    public Integer getConnectionTimeoutSeconds() {
      return connectionTimeoutSeconds;
    }

    public void setConnectionTimeoutSeconds(Integer connectionTimeoutSeconds) {
      this.connectionTimeoutSeconds = connectionTimeoutSeconds;
    }
  }

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

  public Boolean getSsl() {
    return ssl;
  }

  public void setSsl(Boolean ssl) {
    this.ssl = ssl;
  }

  public PoolConfig getPool() {
    return pool;
  }

  public void setPool(PoolConfig pool) {
    this.pool = pool;
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
