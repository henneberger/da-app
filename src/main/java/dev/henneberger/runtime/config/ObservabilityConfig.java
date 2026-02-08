package dev.henneberger.runtime.config;

public class ObservabilityConfig {

  private Boolean enabled;
  private MetricsConfig metrics;
  private TracingConfig tracing;
  private HealthConfig health;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public MetricsConfig getMetrics() {
    return metrics;
  }

  public void setMetrics(MetricsConfig metrics) {
    this.metrics = metrics;
  }

  public TracingConfig getTracing() {
    return tracing;
  }

  public void setTracing(TracingConfig tracing) {
    this.tracing = tracing;
  }

  public HealthConfig getHealth() {
    return health;
  }

  public void setHealth(HealthConfig health) {
    this.health = health;
  }
}
