package io.druid.security.basic.db.entity;

public class BasicAuthenticatorUser
{
  private final String userName;
  private final BasicAuthenticatorCredentials credentials;

  public BasicAuthenticatorUser(
      String userName,
      BasicAuthenticatorCredentials credentials
  )
  {
    this.userName = userName;
    this.credentials = credentials;
  }

  public String getUserName()
  {
    return userName;
  }

  public BasicAuthenticatorCredentials getCredentials()
  {
    return credentials;
  }
}
