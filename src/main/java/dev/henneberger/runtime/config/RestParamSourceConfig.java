package dev.henneberger.runtime.config;

public class RestParamSourceConfig {
  public enum Kind {
    BODY,
    QUERY,
    PATH,
    HEADER,
    JWT,
    ENV,
    RAW_BODY
  }

  private Kind kind;
  private String name;

  public Kind getKind() {
    return kind;
  }

  public void setKind(Kind kind) {
    this.kind = kind;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}

