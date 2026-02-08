package dev.henneberger.runtime.config;

public class KafkaSubscriptionFilterConfig {

  private String field;
  private ParamSourceConfig source;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public ParamSourceConfig getSource() {
    return source;
  }

  public void setSource(ParamSourceConfig source) {
    this.source = source;
  }
}
