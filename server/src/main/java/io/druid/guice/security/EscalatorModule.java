package io.druid.guice.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.druid.guice.JsonConfigProvider;
import io.druid.server.security.Escalator;

public class EscalatorModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.escalator", Escalator.class);
  }
}
