package dev.henneberger.runtime.config;

public class ParamSourceConfig {

  public enum Kind {
    ARG,
    HEADER,
    ENV,
    PARENT,
    JWT,
    CURSOR,
    RAW,
    PYTHON
  }

  private Kind kind;
  private String name;
  private PythonInvocation python;

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

  public PythonInvocation getPython() {
    return python;
  }

  public void setPython(PythonInvocation python) {
    this.python = python;
  }

  public static class PythonInvocation {
    private String functionRef;
    private java.util.List<ParamSourceConfig> args;
    private Integer timeoutMs;

    public String getFunctionRef() {
      return functionRef;
    }

    public void setFunctionRef(String functionRef) {
      this.functionRef = functionRef;
    }

    public java.util.List<ParamSourceConfig> getArgs() {
      return args;
    }

    public void setArgs(java.util.List<ParamSourceConfig> args) {
      this.args = args;
    }

    public Integer getTimeoutMs() {
      return timeoutMs;
    }

    public void setTimeoutMs(Integer timeoutMs) {
      this.timeoutMs = timeoutMs;
    }
  }
}
