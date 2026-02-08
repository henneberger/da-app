package dev.henneberger.runtime.config;

public class SamplingConfig {

  public enum Type {
    ALWAYS_ON,
    ALWAYS_OFF,
    RATIO
  }

  private Type type = Type.ALWAYS_ON;
  private Double value;

  public Type getType() {
    return type == null ? Type.ALWAYS_ON : type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public Double getValue() {
    return value;
  }

  public void setValue(Double value) {
    this.value = value;
  }
}
