package io.druid.security.basic.db.entity;

import io.druid.security.basic.BasicAuthUtils;

public class BasicAuthenticatorCredentials
{
  private final byte[] salt;
  private final byte[] hash;
  private final int iterations;

  public BasicAuthenticatorCredentials(
      byte[] salt,
      byte[] hash,
      int iterations
  )
  {
    this.salt = salt;
    this.hash = hash;
    this.iterations = iterations;
  }

  public BasicAuthenticatorCredentials(char[] password)
  {
    this.iterations = BasicAuthUtils.KEY_ITERATIONS;
    this.salt = BasicAuthUtils.generateSalt();
    this.hash = BasicAuthUtils.hashPassword(password, salt, iterations);
  }

  public byte[] getSalt()
  {
    return salt;
  }

  public byte[] getHash()
  {
    return hash;
  }

  public int getIterations()
  {
    return iterations;
  }
}
