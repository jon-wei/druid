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
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.authentication.db.BasicAuthenticatorCommonCacheConfig;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.cache.NoopBasicAuthorizerCacheNotifier;
import io.druid.security.basic.authorization.db.updater.CoordinatorBasicAuthorizerMetadataStorageUpdater;
import io.druid.security.basic.authorization.endpoint.BasicAuthorizerResource;
import io.druid.security.basic.authorization.endpoint.CoordinatorBasicAuthorizerResourceHandler;
import io.druid.server.security.AuthorizerMapper;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.servlet.http.HttpServletRequest;

public class CoordinatorBasicAuthorizerResourceTest
{
  private final static String AUTHORIZER_NAME = "test";
  private final static String AUTHORIZER_NAME2 = "test2";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private BasicAuthorizerResource resource;
  private CoordinatorBasicAuthorizerMetadataStorageUpdater storageUpdater;
  private HttpServletRequest req;

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);

    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    AuthorizerMapper authorizerMapper = new AuthorizerMapper(
        ImmutableMap.of(
            AUTHORIZER_NAME,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME,
                null,
                null,
                null
            ),
            AUTHORIZER_NAME2,
            new BasicRoleBasedAuthorizer(
                null,
                AUTHORIZER_NAME2,
                null,
                null,
                null
            )
        )
    );

    storageUpdater = new CoordinatorBasicAuthorizerMetadataStorageUpdater(
        authorizerMapper,
        connector,
        tablesConfig,
        new BasicAuthenticatorCommonCacheConfig(null, null),
        new ObjectMapper(new SmileFactory()),
        new NoopBasicAuthorizerCacheNotifier(),
        null
    );

    resource = new BasicAuthorizerResource(
        new CoordinatorBasicAuthorizerResourceHandler(
            storageUpdater,
            authorizerMapper
        )
    );

    storageUpdater.start();
  }

  @After
  public void tearDown() throws Exception
  {
    storageUpdater.stop();
  }

  @Test
  public void testSeparateDatabaseTables()
  {
    Response response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableList.of(), response.getEntity());

    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid2");
    resource.createUser(req, BASIC_AUTHORIZER_NAME, "druid3");

    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid4");
    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid5");
    resource.createUser(req, BASIC_AUTHORIZER_NAME2, "druid6");

    List<Map<String, Object>> expectedUsers = ImmutableList.of(
        ImmutableMap.of("name", "druid"),
        ImmutableMap.of("name", "druid2"),
        ImmutableMap.of("name", "druid3")
    );
    List<Map<String, Object>> expectedUsers2 = ImmutableList.of(
        ImmutableMap.of("name", "druid4"),
        ImmutableMap.of("name", "druid5"),
        ImmutableMap.of("name", "druid6")
    );

    response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers, response.getEntity());

    response = resource.getAllUsers(req, BASIC_AUTHORIZER_NAME2);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(expectedUsers2, response.getEntity());
  }
}
