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

package io.druid.security.authorization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.db.BasicAuthenticatorCommonCacheConfig;
import io.druid.security.basic.authentication.db.cache.NoopBasicAuthenticatorCacheNotifier;
import io.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.cache.NoopBasicAuthorizerCacheNotifier;
import io.druid.security.basic.authorization.db.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.db.entity.BasicAuthorizerUser;
import io.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import io.druid.server.security.AuthenticatorMapper;
import io.druid.server.security.AuthorizerMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class CoordinatorBasicAuthorizerMetadataStorageUpdaterTest
{
  private final static String AUTHORIZER_NAME = "test";
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater updater;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();
    //injector = setupInjector();

    updater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        new AuthorizerMapper(
            ImmutableMap.of(
                AUTHORIZER_NAME,
                new BasicRoleBasedAuthorizer(
                    null,
                    AUTHORIZER_NAME,
                    null,
                    null,
                    null
                )
            )
        ),
        connector,
        tablesConfig,
        new BasicAuthenticatorCommonCacheConfig(null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    updater.start();
  }

  // user tests
  @Test
  public void testCreateDeleteUser() throws Exception
  {
    updater.createUser(AUTHORIZER_NAME, "druid");
    Map<String, BasicAuthorizerUser> expectedUserMap = ImmutableMap.of(
        AUTHORIZER_NAME, new BasicAuthorizerUser("druid", ImmutableSet.of())
    );

    Map<String, BasicAuthorizerUser> actualUserMap = updater.deserializeUserMap(
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);

    updater.deleteUser(AUTHORIZER_NAME, "druid");
    actualUserMap = updater.deserializeUserMap(
        updater.getCurrentUserMapBytes(AUTHORIZER_NAME)
    );
    Assert.assertEquals(expectedUserMap, actualUserMap);
  }
}
