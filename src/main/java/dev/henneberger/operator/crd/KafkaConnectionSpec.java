package dev.henneberger.operator.crd;

public class KafkaConnectionSpec {

  private String bootstrapServers;
  private String clientId;
  private String acks = "all";
  private Integer lingerMs = 5;
  private Integer batchSize = 16384;
  private Integer replicas = 1;
  private Integer servicePort = 8080;
  private String image;

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public void setBootstrapServers(String bootstrapServers) {
    this.bootstrapServers = bootstrapServers;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getAcks() {
    return acks;
  }

  public void setAcks(String acks) {
    this.acks = acks;
  }

  public Integer getLingerMs() {
    return lingerMs;
  }

  public void setLingerMs(Integer lingerMs) {
    this.lingerMs = lingerMs;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(Integer batchSize) {
    this.batchSize = batchSize;
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
