package io.druid.server.security;

import com.metamx.http.client.HttpClient;

public class AllowAllEscalator implements Escalator
{
  @Override
  public HttpClient createEscalatedClient(HttpClient baseClient)
  {
    return baseClient;
  }

  @Override
  public org.eclipse.jetty.client.HttpClient createEscalatedJettyClient(org.eclipse.jetty.client.HttpClient baseClient)
  {
    return baseClient;
  }

  @Override
  public AuthenticationResult createEscalatedAuthenticationResult()
  {
    return AllowAllAuthenticator.ALLOW_ALL_RESULT;
  }
}
