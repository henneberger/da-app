package dev.henneberger.runtime.config;

import java.util.Map;

public class OtlpTracingConfig {

  public enum Protocol {
    GRPC,
    HTTP_PROTOBUF
  }

  private String endpoint;
  private Protocol protocol = Protocol.GRPC;
  private Map<String, String> headers;

  public String getEndpoint() {
    return endpoint;
  }

  public void setEndpoint(String endpoint) {
    this.endpoint = endpoint;
  }

  public Protocol getProtocol() {
    return protocol == null ? Protocol.GRPC : protocol;
  }

  public void setProtocol(Protocol protocol) {
    this.protocol = protocol;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }
}
