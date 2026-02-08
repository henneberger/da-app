package dev.henneberger.runtime.config;

import java.util.List;

public class HealthConfig {

  private Boolean enabled;
  private String endpoint;
  private List<HealthCheckConfig> checks;

  public Boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(Boolean enabled) {
    this.enabled = enabled;
  }

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public List<HealthCheckConfig> getChecks() {
    return checks;
  }

  public void setChecks(List<HealthCheckConfig> checks) {
    this.checks = checks;
  }
}
