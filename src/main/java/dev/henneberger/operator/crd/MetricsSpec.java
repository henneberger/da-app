package dev.henneberger.operator.crd;

import java.util.Map;

public class MetricsSpec {

  public enum Backend {
    PROMETHEUS,
    OTLP
  }

  private Boolean enabled;
  private Backend backend = Backend.PROMETHEUS;
  private String endpoint;
  private Boolean jvm;
  private Boolean httpServer;
  private Boolean httpClient;
  private Map<String, String> labels;
  private OtlpMetricsSpec otlp;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Backend getBackend() {
    return backend == null ? Backend.PROMETHEUS : backend;
  }

  public void setBackend(Backend backend) {
    this.backend = backend;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public Boolean getJvm() {
    return jvm;
  }

  public void setJvm(Boolean jvm) {
    this.jvm = jvm;
  }

  public Boolean getHttpServer() {
    return httpServer;
  }

  public void setHttpServer(Boolean httpServer) {
    this.httpServer = httpServer;
  }

  public Boolean getHttpClient() {
    return httpClient;
  }

  public void setHttpClient(Boolean httpClient) {
    this.httpClient = httpClient;
  }

  public Map<String, String> getLabels() {
    return labels;
  }

  public void setLabels(Map<String, String> labels) {
    this.labels = labels;
  }

  public OtlpMetricsSpec getOtlp() {
    return otlp;
  }

  public void setOtlp(OtlpMetricsSpec otlp) {
    this.otlp = otlp;
  }
}
