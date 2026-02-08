package dev.henneberger.operator.crd;

public class ObservabilitySpec {

  private Boolean enabled;
  private MetricsSpec metrics;
  private TracingSpec tracing;
  private HealthSpec health;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public MetricsSpec getMetrics() {
    return metrics;
  }

  public void setMetrics(MetricsSpec metrics) {
    this.metrics = metrics;
  }

  public TracingSpec getTracing() {
    return tracing;
  }

  public void setTracing(TracingSpec tracing) {
    this.tracing = tracing;
  }

  public HealthSpec getHealth() {
    return health;
  }

  public void setHealth(HealthSpec health) {
    this.health = health;
  }
}
