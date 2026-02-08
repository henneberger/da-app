package dev.henneberger.operator.crd;

public class QueryParam {

  private int index;
  private ParamSource source;

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

  public ParamSource getSource() {
    return source;
  }

  public void setSource(ParamSource source) {
    this.source = source;
  }
}
