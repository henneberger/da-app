package dev.henneberger.operator.crd;

import java.util.List;

public class PythonFunctionSpec {
  private List<String> schemaRefs;
  private Requirements requirements;
  private Function function;
  private Integer timeoutMs;

  public List<String> getSchemaRefs() {
    return schemaRefs;
  }

  public void setSchemaRefs(List<String> schemaRefs) {
    this.schemaRefs = schemaRefs;
  }

  public Requirements getRequirements() {
    return requirements;
  }

  public void setRequirements(Requirements requirements) {
    this.requirements = requirements;
  }

  public Function getFunction() {
    return function;
  }

  public void setFunction(Function function) {
    this.function = function;
  }

  public Integer getTimeoutMs() {
    return timeoutMs;
  }

  public void setTimeoutMs(Integer timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  public static class Requirements {
    private String inline;

    public String getInline() {
      return inline;
    }

    public void setInline(String inline) {
      this.inline = inline;
    }
  }

  public static class Function {
    private String name;
    private Module module;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public Module getModule() {
      return module;
    }

    public void setModule(Module module) {
      this.module = module;
    }
  }

  public static class Module {
    private String inline;

    public String getInline() {
      return inline;
    }

    public void setInline(String inline) {
      this.inline = inline;
    }
  }
}

