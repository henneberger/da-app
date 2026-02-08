package dev.henneberger.operator.crd;

import java.util.Map;

public class DynamoDbQuerySpec {

  private String table;
  private String keyConditionExpression;
  private Map<String, String> expressionAttributeNames;
  private Map<String, Object> expressionAttributeValues;
  private String indexName;
  private Integer limit;

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getKeyConditionExpression() {
    return keyConditionExpression;
  }

  public void setKeyConditionExpression(String keyConditionExpression) {
    this.keyConditionExpression = keyConditionExpression;
  }

  public Map<String, String> getExpressionAttributeNames() {
    return expressionAttributeNames;
  }

  public void setExpressionAttributeNames(Map<String, String> expressionAttributeNames) {
    this.expressionAttributeNames = expressionAttributeNames;
  }

  public Map<String, Object> getExpressionAttributeValues() {
    return expressionAttributeValues;
  }

  public void setExpressionAttributeValues(Map<String, Object> expressionAttributeValues) {
    this.expressionAttributeValues = expressionAttributeValues;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }
}
