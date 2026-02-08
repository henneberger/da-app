package dev.henneberger.operator.crd;

public class MongoQuerySpec {

  private String collection;
  private String pipeline;

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    this.collection = collection;
  }

  public String getPipeline() {
    return pipeline;
  }

  public void setPipeline(String pipeline) {
    this.pipeline = pipeline;
  }
}
