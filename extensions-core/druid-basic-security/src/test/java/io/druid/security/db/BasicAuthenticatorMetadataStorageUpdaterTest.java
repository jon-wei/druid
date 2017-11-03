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
import com.google.common.collect.ImmutableMap;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Map;

public class BasicAuthenticatorMetadataStorageUpdaterTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig;
  private BasicAuthenticatorMetadataStorageUpdater updater;

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();
    connector.createConfigTable();

    updater = new BasicAuthenticatorMetadataStorageUpdater(
        "test",
        connector,
        tablesConfig,
        new ObjectMapper(new SmileFactory())
    );
  }

  @Test
  public void createUser()
  {
    updater.createUser("druid");
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of(
        "druid", new BasicAuthenticatorUser("druid", null)
    );
    Map<String, BasicAuthenticatorUser> actualUserMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes());
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // create duplicate should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] already exists.");
    updater.createUser("druid");
  }

  @Test
  public void deleteUser()
  {
    updater.createUser("druid");
    updater.deleteUser("druid");
    Map<String, BasicAuthenticatorUser> expectedUserMap = ImmutableMap.of();
    Map<String, BasicAuthenticatorUser> actualUserMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes());
    Assert.assertEquals(expectedUserMap, actualUserMap);

    // delete non-existent user should fail
    expectedException.expect(BasicSecurityDBResourceException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    updater.deleteUser("druid");
  }

  @Test
  public void setCredentials()
  {
    updater.createUser("druid");
    updater.setUserCredentials("druid", "helloworld".toCharArray());

    Map<String, BasicAuthenticatorUser> userMap = updater.deserializeUserMap(updater.getCurrentUserMapBytes());
    BasicAuthenticatorCredentials credentials = userMap.get("druid").getCredentials();

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        "helloworld".toCharArray(),
        credentials.getSalt(),
        credentials.getIterations()
    );

    Assert.assertArrayEquals(credentials.getHash(), recalculatedHash);
  }
}
