package dev.henneberger.operator;

public class FlinkSqlJobSpec {
  private String image;
  private String sql;
  private Integer parallelism;
  private String flinkVersion;
  private String jobName;
  private String serviceAccount;
  private java.util.List<String> pythonRefs;
  private java.util.List<String> catalogRefs;
  private Overrides overrides;
  private Schedule schedule;

  public String getImage() {
    return image;
  }

  public void setImage(String image) {
    this.image = image;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public Integer getParallelism() {
    return parallelism;
  }

  public void setParallelism(Integer parallelism) {
    this.parallelism = parallelism;
  }

  public String getFlinkVersion() {
    return flinkVersion;
  }

  public void setFlinkVersion(String flinkVersion) {
    this.flinkVersion = flinkVersion;
  }

  public String getJobName() {
    return jobName;
  }

  public void setJobName(String jobName) {
    this.jobName = jobName;
  }

  public String getServiceAccount() {
    return serviceAccount;
  }

  public void setServiceAccount(String serviceAccount) {
    this.serviceAccount = serviceAccount;
  }

  public java.util.List<String> getPythonRefs() {
    return pythonRefs;
  }

  public void setPythonRefs(java.util.List<String> pythonRefs) {
    this.pythonRefs = pythonRefs;
  }

  public java.util.List<String> getCatalogRefs() {
    return catalogRefs;
  }

  public void setCatalogRefs(java.util.List<String> catalogRefs) {
    this.catalogRefs = catalogRefs;
  }

  public Overrides getOverrides() {
    return overrides;
  }

  public void setOverrides(Overrides overrides) {
    this.overrides = overrides;
  }

  public Schedule getSchedule() {
    return schedule;
  }

  public void setSchedule(Schedule schedule) {
    this.schedule = schedule;
  }


  public static class Schedule {
    private String maxRunTime;
    private String rekickInterval;
    private Integer priority;

    public String getMaxRunTime() {
      return maxRunTime;
    }

    public void setMaxRunTime(String maxRunTime) {
      this.maxRunTime = maxRunTime;
    }

    public String getRekickInterval() {
      return rekickInterval;
    }

    public void setRekickInterval(String rekickInterval) {
      this.rekickInterval = rekickInterval;
    }

    public Integer getPriority() {
      return priority;
    }

    public void setPriority(Integer priority) {
      this.priority = priority;
    }
  }

  public static class Overrides {
    private java.util.Map<String, Object> merge;
    private java.util.List<java.util.Map<String, Object>> patches;

    public java.util.Map<String, Object> getMerge() {
      return merge;
    }

    public void setMerge(java.util.Map<String, Object> merge) {
      this.merge = merge;
    }

    public java.util.List<java.util.Map<String, Object>> getPatches() {
      return patches;
    }

    public void setPatches(java.util.List<java.util.Map<String, Object>> patches) {
      this.patches = patches;
    }
  }

}
