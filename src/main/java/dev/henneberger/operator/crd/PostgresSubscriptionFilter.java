package dev.henneberger.operator.crd;

public class PostgresSubscriptionFilter {

  private String field;
  private ParamSource source;

  public String getField() {
    return field;
  }

  public void setField(String field) {
    this.field = field;
  }

  public ParamSource getSource() {
    return source;
  }

  public void setSource(ParamSource source) {
    this.source = source;
  }
}
