package dev.henneberger.operator.crd;

public class PythonFunctionStatus {
  private String state;
  private String message;

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
}

