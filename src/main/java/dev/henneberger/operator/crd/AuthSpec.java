package dev.henneberger.operator.crd;

public class AuthSpec {

  public enum Type {
    NONE,
    JWT,
    BASIC
  }

  private Type type = Type.NONE;
  private Boolean required = Boolean.TRUE;
  private String tokenHeader;
  private String tokenCookie;
  private String tokenQueryParam;
  private JwtAuthSpec jwt;
  private java.util.List<BasicAuthUserSpec> basicUsers;

  public Type getType() {
    return type == null ? Type.NONE : type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public Boolean getRequired() {
    return required;
  }

  public void setRequired(Boolean required) {
    this.required = required;
  }

  public String getTokenHeader() {
    return tokenHeader;
  }

  public void setTokenHeader(String tokenHeader) {
    this.tokenHeader = tokenHeader;
  }

  public String getTokenCookie() {
    return tokenCookie;
  }

  public void setTokenCookie(String tokenCookie) {
    this.tokenCookie = tokenCookie;
  }

  public String getTokenQueryParam() {
    return tokenQueryParam;
  }

  public void setTokenQueryParam(String tokenQueryParam) {
    this.tokenQueryParam = tokenQueryParam;
  }

  public JwtAuthSpec getJwt() {
    return jwt;
  }

  public void setJwt(JwtAuthSpec jwt) {
    this.jwt = jwt;
  }

  public java.util.List<BasicAuthUserSpec> getBasicUsers() {
    return basicUsers;
  }

  public void setBasicUsers(java.util.List<BasicAuthUserSpec> basicUsers) {
    this.basicUsers = basicUsers;
  }
}
