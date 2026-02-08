package dev.henneberger.runtime.config;

import java.util.List;

public class PaginationConfig {

  public enum Mode {
    CURSOR
  }

  private Mode mode = Mode.CURSOR;
  private String firstArg;
  private String afterArg;
  private Integer maxPageSize;
  private Boolean fetchExtra = Boolean.TRUE;
  private List<String> cursorFields;
  private String edgesFieldName;
  private String nodeFieldName;
  private String cursorFieldName;
  private String pageInfoFieldName;
  private String endCursorFieldName;
  private String hasNextPageFieldName;
  private CursorSigningConfig signing;

  public Mode getMode() {
    return mode == null ? Mode.CURSOR : mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public String getFirstArg() {
    return firstArg;
  }

  public void setFirstArg(String firstArg) {
    this.firstArg = firstArg;
  }

  public String getAfterArg() {
    return afterArg;
  }

  public void setAfterArg(String afterArg) {
    this.afterArg = afterArg;
  }

  public Integer getMaxPageSize() {
    return maxPageSize;
  }

  public void setMaxPageSize(Integer maxPageSize) {
    this.maxPageSize = maxPageSize;
  }

  public Boolean getFetchExtra() {
    return fetchExtra;
  }

  public void setFetchExtra(Boolean fetchExtra) {
    this.fetchExtra = fetchExtra;
  }

  public List<String> getCursorFields() {
    return cursorFields;
  }

  public void setCursorFields(List<String> cursorFields) {
    this.cursorFields = cursorFields;
  }

  public String getEdgesFieldName() {
    return edgesFieldName;
  }

  public void setEdgesFieldName(String edgesFieldName) {
    this.edgesFieldName = edgesFieldName;
  }

  public String getNodeFieldName() {
    return nodeFieldName;
  }

  public void setNodeFieldName(String nodeFieldName) {
    this.nodeFieldName = nodeFieldName;
  }

  public String getCursorFieldName() {
    return cursorFieldName;
  }

  public void setCursorFieldName(String cursorFieldName) {
    this.cursorFieldName = cursorFieldName;
  }

  public String getPageInfoFieldName() {
    return pageInfoFieldName;
  }

  public void setPageInfoFieldName(String pageInfoFieldName) {
    this.pageInfoFieldName = pageInfoFieldName;
  }

  public String getEndCursorFieldName() {
    return endCursorFieldName;
  }

  public void setEndCursorFieldName(String endCursorFieldName) {
    this.endCursorFieldName = endCursorFieldName;
  }

  public String getHasNextPageFieldName() {
    return hasNextPageFieldName;
  }

  public void setHasNextPageFieldName(String hasNextPageFieldName) {
    this.hasNextPageFieldName = hasNextPageFieldName;
  }

  public CursorSigningConfig getSigning() {
    return signing;
  }

  public void setSigning(CursorSigningConfig signing) {
    this.signing = signing;
  }
}
