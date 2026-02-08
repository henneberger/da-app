package dev.henneberger.operator.crd;

import java.util.List;

public class HealthSpec {

  private Boolean enabled;
  private String endpoint;
  private List<HealthCheckSpec> checks;

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

  public List<HealthCheckSpec> getChecks() {
    return checks;
  }

  public void setChecks(List<HealthCheckSpec> checks) {
    this.checks = checks;
  }
}
