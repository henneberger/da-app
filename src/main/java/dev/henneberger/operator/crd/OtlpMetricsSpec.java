package dev.henneberger.operator.crd;

import java.util.Map;

public class OtlpMetricsSpec {

  private String endpoint;
  private Map<String, String> headers;
  private Integer stepSeconds;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  public Integer getStepSeconds() {
    return stepSeconds;
  }

  public void setStepSeconds(Integer stepSeconds) {
    this.stepSeconds = stepSeconds;
  }
}
