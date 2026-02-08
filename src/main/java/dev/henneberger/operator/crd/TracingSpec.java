package dev.henneberger.operator.crd;

public class TracingSpec {

  public enum Exporter {
    OTLP,
    ZIPKIN,
    JAEGER
  }

  private Boolean enabled;
  private Exporter exporter = Exporter.OTLP;
  private String serviceName;
  private OtlpTracingSpec otlp;
  private SamplingSpec sampling;

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

  public OtlpTracingSpec getOtlp() {
    return otlp;
  }

  public void setOtlp(OtlpTracingSpec otlp) {
    this.otlp = otlp;
  }

  public SamplingSpec getSampling() {
    return sampling;
  }

  public void setSampling(SamplingSpec sampling) {
    this.sampling = sampling;
  }
}
