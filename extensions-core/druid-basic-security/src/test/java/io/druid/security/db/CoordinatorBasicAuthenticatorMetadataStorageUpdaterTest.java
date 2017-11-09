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

package io.druid.security.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.initialization.Initialization;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.db.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.cache.CoordinatorBasicAuthenticatorCacheNotifier;
import io.druid.security.basic.db.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.DruidNode;
import io.druid.server.security.AuthenticatorMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class CoordinatorBasicAuthenticatorMetadataStorageUpdaterTest
{
  private final static String AUTHENTICATOR_NAME = "test";
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private Injector injector;
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private CoordinatorBasicAuthenticatorMetadataStorageUpdater updater;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();
    injector = setupInjector();

    CoordinatorBasicAuthenticatorCacheNotifier notifier = new CoordinatorBasicAuthenticatorCacheNotifier(null, null);
    updater = new CoordinatorBasicAuthenticatorMetadataStorageUpdater(
        injector,
        connector,
        tablesConfig,
        new ObjectMapper(new SmileFactory()),
        notifier
    );

    notifier.start();
    updater.start();
  }

  @Test
  public void createUser()
  {
    updater.createUser(AUTHENTICATOR_NAME, "druid");
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "druid", new BasicAuthenticatorUser("druid", null)
    );
    Map<String, BasicAuthenticatorUser> actualUserMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // create duplicate should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already exists.");
    updater.createUser(AUTHENTICATOR_NAME, "druid");
  }

  @Test
  public void deleteUser()
  {
    updater.createUser(AUTHENTICATOR_NAME, "druid");
    updater.deleteUser(AUTHENTICATOR_NAME, "druid");
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of();
    Map<String, BasicAuthenticatorUser> actualUserMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // delete non-existent user should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    updater.deleteUser(AUTHENTICATOR_NAME, "druid");
  }

  @Test
  public void setCredentials()
  {
    updater.createUser(AUTHENTICATOR_NAME, "druid");
    updater.setUserCredentials(AUTHENTICATOR_NAME, "druid", "helloworld".toCharArray());

    Map<String, BasicAuthenticatorUser> userMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes(AUTHENTICATOR_NAME));
    BasicAuthenticatorCredentials credentials = userMap.get("druid").getCredentials();

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        credentials.getSalt(),
        credentials.getIterations()
    );

    Assert.assertArrayEquals(credentials.getHash(), recalculatedHash);
  }

  private Injector setupInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder,
                    Key.get(DruidNode.class, Self.class),
                    new DruidNode("test", "localhost", null, null, true, false)
                );

                binder.bind(AuthenticatorMapper.class).toInstance(
                    new AuthenticatorMapper(
                        ImmutableMap.of(
                            "test",
                            new BasicHTTPAuthenticator(
                                null,
                                injector,
                                "test",
                                null,
                                null,
                                null,
                                null,
                                "druid"
                            )
                        ),
                        "test"
                    )
                );
              }
            }
        )
    );
  }
}
