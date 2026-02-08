package dev.henneberger.operator.crd;

public class ConnectionSpec {

  private String host;
  private Integer port = 5432;
  private String database;
  private String user;
  private String password;
  private SecretKeyRef passwordSecretRef;
  private Boolean ssl = false;
  private PoolSpec pool;
  private Integer replicas = 1;
  private Integer servicePort = 8080;
  private String image;

  public static class PoolSpec {
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

  public Boolean getSsl() {
    return ssl;
  }

  public void setSsl(Boolean ssl) {
    this.ssl = ssl;
  }

  public PoolSpec getPool() {
    return pool;
  }

  public void setPool(PoolSpec pool) {
    this.pool = pool;
  }

  public Integer getReplicas() {
    return replicas;
  }

  public void setReplicas(Integer replicas) {
    this.replicas = replicas;
  }

  public Integer getServicePort() {
    return servicePort;
  }

  public void setServicePort(Integer servicePort) {
    this.servicePort = servicePort;
  }

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }
}
