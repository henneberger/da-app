package dev.henneberger.operator;

public class FlinkSqlJobStatus {
  private String state;
  private String message;
  private String flinkDeploymentName;
  private String activeStartTime;
  private String lastKickTime;
  private String lastStopTime;
  private String lastSavepointPath;

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getFlinkDeploymentName() {
    return flinkDeploymentName;
  }

  public void setFlinkDeploymentName(String flinkDeploymentName) {
    this.flinkDeploymentName = flinkDeploymentName;
  }

  public String getActiveStartTime() {
    return activeStartTime;
  }

  public void setActiveStartTime(String activeStartTime) {
    this.activeStartTime = activeStartTime;
  }

  public String getLastKickTime() {
    return lastKickTime;
  }

  public void setLastKickTime(String lastKickTime) {
    this.lastKickTime = lastKickTime;
  }

  public String getLastStopTime() {
    return lastStopTime;
  }

  public void setLastStopTime(String lastStopTime) {
    this.lastStopTime = lastStopTime;
  }

  public String getLastSavepointPath() {
    return lastSavepointPath;
  }

  public void setLastSavepointPath(String lastSavepointPath) {
    this.lastSavepointPath = lastSavepointPath;
  }
}
