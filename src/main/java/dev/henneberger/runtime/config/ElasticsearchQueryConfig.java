package dev.henneberger.runtime.config;

public class ElasticsearchQueryConfig {
  private String index;
  private String query;

  public String getIndex() {
    return index;
  }

  public void setIndex(String index) {
    this.index = index;
  }

  public String getQuery() {
    return query;
  }

  public void setQuery(String query) {
    this.query = query;
  }
}
