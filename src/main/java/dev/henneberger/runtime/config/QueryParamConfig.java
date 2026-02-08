package dev.henneberger.runtime.config;

public class QueryParamConfig {

  private int index;
  private ParamSourceConfig source;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public ParamSourceConfig getSource() {
    return source;
  }

  public void setSource(ParamSourceConfig source) {
    this.source = source;
  }
}
