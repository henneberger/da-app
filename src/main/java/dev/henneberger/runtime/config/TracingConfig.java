package dev.henneberger.runtime.config;

public class TracingConfig {

  public enum Exporter {
    OTLP,
    ZIPKIN,
    JAEGER
  }

  private Boolean enabled;
  private Exporter exporter = Exporter.OTLP;
  private String serviceName;
  private OtlpTracingConfig otlp;
  private SamplingConfig sampling;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public Exporter getExporter() {
    return exporter == null ? Exporter.OTLP : exporter;
  }

  public void setExporter(Exporter exporter) {
    this.exporter = exporter;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public OtlpTracingConfig getOtlp() {
    return otlp;
  }

  public void setOtlp(OtlpTracingConfig otlp) {
    this.otlp = otlp;
  }

  public SamplingConfig getSampling() {
    return sampling;
  }

  public void setSampling(SamplingConfig sampling) {
    this.sampling = sampling;
  }
}
