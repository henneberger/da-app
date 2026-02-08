package dev.henneberger.operator.crd;

public class GraphQLSchemaSpec {

  private String schema;
  private Integer replicas = 1;
  private Integer servicePort = 8080;
  private String image;
  private AuthSpec auth;
  private ObservabilitySpec observability;

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
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

  public AuthSpec getAuth() {
    return auth;
  }

  public void setAuth(AuthSpec auth) {
    this.auth = auth;
  }

  public ObservabilitySpec getObservability() {
    return observability;
  }

  public void setObservability(ObservabilitySpec observability) {
    this.observability = observability;
  }
}
