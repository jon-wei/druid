/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.security.basic;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.name.Names;
import io.druid.guice.Jerseys;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.authentication.BasicAuthenticatorResource;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.CoordinatorBasicAuthenticatorResource;
import io.druid.security.basic.authentication.DefaultBasicAuthenticatorResource;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.NoopBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.cache.BasicAuthenticatorCacheManager;
import io.druid.security.basic.db.cache.CoordinatorBasicAuthenticatorCacheManager;
import io.druid.security.basic.db.cache.DefaultBasicAuthenticatorCacheManager;

import java.util.List;
import java.util.Properties;

public class BasicSecurityDruidModule implements DruidModule
{
  //@Inject
  //@Named("serviceName")
  //private String serviceName;

  @Inject
  private Properties props;

  public final String STORAGE_CONNECTOR_TYPE_PROPERTY = "druid.metadata.storage.type";

  @Override
  public void configure(Binder binder)
  {
    LifecycleModule.register(binder, BasicAuthenticatorMetadataStorageUpdater.class);
    LifecycleModule.register(binder, BasicAuthenticatorCacheManager.class);

    if (isCoordinatorBinderHack()) {
      Jerseys.addResource(binder, CoordinatorBasicAuthenticatorResource.class);
    } else {
      Jerseys.addResource(binder, DefaultBasicAuthenticatorResource.class);
    }

    /*
    binder.bind(BasicAuthenticatorResource.class)
          .toProvider(new BasicAuthenticatorResourceProvider())
          .in(LazySingleton.class);
    Jerseys.addResource(binder, BasicAuthenticatorResource.class);
    */

    /*
    Multibinder.newSetBinder(binder, BasicAuthenticatorResource.class, JSR311Resource.class)
               .addBinding()
               .toProvider(BasicAuthenticatorResourceProvider.class)
               .in(LazySingleton.class);
               */

    /*
    Multibinder.newSetBinder(binder, new TypeLiteral<BasicAuthenticatorResource>(){}, JSR311Resource.class)
               .addBinding()
               .toProvider(BasicAuthenticatorResourceProvider.class);
               */

    //Jerseys.addResource(binder, CoordinatorBasicAuthenticatorResource.class);

    //Jerseys.addResource(binder, BasicAuthenticatorResourceProvider.class);
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorMetadataStorageUpdater createAuthenticatorStorageUpdater(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorMetadataStorageUpdater.class);
    } else {
      return injector.getInstance(NoopBasicAuthenticatorMetadataStorageUpdater.class);
    }
  }

  @Provides @LazySingleton
  public static BasicAuthenticatorCacheManager createAuthenticatorCacheManager(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorCacheManager.class);
    } else {
      return injector.getInstance(DefaultBasicAuthenticatorCacheManager.class);
    }
  }

  //@Provides @LazySingleton
  public static BasicAuthenticatorResource createAuthenticatorResource(final Injector injector)
  {
    if (isCoordinator(injector)) {
      return injector.getInstance(CoordinatorBasicAuthenticatorResource.class);
    } else {
      return injector.getInstance(DefaultBasicAuthenticatorResource.class);
    }
  }

  private static class BasicAuthenticatorResourceProvider implements Provider<BasicAuthenticatorResource>
  {
    private BasicAuthenticatorResource resource;

    @Inject
    public void inject(Injector injector)
    {
      resource = createAuthenticatorResource(injector);
    }

    @Override
    public BasicAuthenticatorResource get()
    {
      return resource;
    }
  }


  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new SimpleModule("BasicDruidSecurity").registerSubtypes(
            BasicHTTPAuthenticator.class,
            BasicRoleBasedAuthorizer.class
        )
    );
  }

  private static boolean isCoordinator(Injector injector)
  {
    final String serviceName;
    try {
      serviceName = injector.getInstance(Key.get(String.class, Names.named("serviceName")));
    }
    catch (Exception e) {
      return false;
    }

    return "druid/coordinator".equals(serviceName);
  }

  private boolean isCoordinatorBinderHack()
  {
    String javaCmd = props.getProperty("sun.java.command");
    if (javaCmd == null) {
      return false;
    }

    javaCmd = StringUtils.toLowerCase(javaCmd);
    return javaCmd.contains("coordinator");
  }
}
