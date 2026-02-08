package dev.henneberger.runtime.config;

public class MongoQueryConfig {
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
