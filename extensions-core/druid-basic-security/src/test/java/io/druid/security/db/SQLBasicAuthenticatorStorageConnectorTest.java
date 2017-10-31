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

import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.BasicAuthUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.Map;

public class SQLBasicAuthenticatorStorageConnectorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyAuthenticatorStorageConnector.DerbyConnectorRule authenticatorRule =
      new TestDerbyAuthenticatorStorageConnector.DerbyConnectorRule("test");

  private TestDerbyAuthenticatorStorageConnector authenticatorConnector;

  @Before
  public void setUp() throws Exception
  {
    authenticatorConnector = authenticatorRule.getConnector();
    createAllTables();
  }

  @After
  public void tearDown() throws Exception
  {
    dropAllTables();
  }

  @Test
  public void testCreateTables() throws Exception
  {
    authenticatorConnector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            for (String table : authenticatorConnector.getTableNames()) {
              Assert.assertTrue(
                  StringUtils.format("authentication table %s was not created!", table),
                  authenticatorConnector.tableExists(handle, table)
              );
            }

            return null;
          }
        }
    );
  }

  // user tests
  @Test
  public void testCreateDeleteUser() throws Exception
  {
    authenticatorConnector.createUser("druid");
    Map<String, Object> expectedUser = ImmutableMap.of(
        "name", "druid"
    );
    Map<String, Object> dbUser = authenticatorConnector.getUser("druid");
    Assert.assertEquals(expectedUser, dbUser);

    authenticatorConnector.deleteUser("druid");
    dbUser = authenticatorConnector.getUser("druid");
    Assert.assertEquals(null, dbUser);
  }

  @Test
  public void testDeleteNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    authenticatorConnector.deleteUser("druid");
  }

  @Test
  public void testCreateDuplicateUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] already exists.");
    authenticatorConnector.createUser("druid");
    authenticatorConnector.createUser("druid");
  }

  // user credentials
  @Test
  public void testAddUserCredentials() throws Exception
  {
    char[] pass = "blah".toCharArray();
    authenticatorConnector.createUser("druid");
    authenticatorConnector.setUserCredentials("druid", pass);
    Assert.assertTrue(authenticatorConnector.checkCredentials("druid", pass));
    Assert.assertFalse(authenticatorConnector.checkCredentials("druid", "wrongPass".toCharArray()));

    Map<String, Object> creds = authenticatorConnector.getUserCredentials("druid");
    Assert.assertEquals("druid", creds.get("user_name"));
    byte[] salt = (byte[]) creds.get("salt");
    byte[] hash = (byte[]) creds.get("hash");
    int iterations = (Integer) creds.get("iterations");
    Assert.assertEquals(BasicAuthUtils.SALT_LENGTH, salt.length);
    Assert.assertEquals(BasicAuthUtils.KEY_LENGTH / 8, hash.length);
    Assert.assertEquals(BasicAuthUtils.KEY_ITERATIONS, iterations);

    byte[] recalculatedHash = BasicAuthUtils.hashPassword(
        pass,
        salt,
        iterations
    );
    Assert.assertArrayEquals(recalculatedHash, hash);
  }

  @Test
  public void testAddCredentialsToNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    char[] pass = "blah".toCharArray();
    authenticatorConnector.setUserCredentials("druid", pass);
  }

  @Test
  public void testGetCredentialsForNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    authenticatorConnector.getUserCredentials("druid");
  }


  private void createAllTables()
  {
    authenticatorConnector.createUserTable();
    authenticatorConnector.createUserCredentialsTable();
  }

  private void dropAllTables()
  {
    for (String table : authenticatorConnector.getTableNames()) {
      dropAuthenticatorTable(table);
    }
  }

  private void dropAuthenticatorTable(final String tableName)
  {
    authenticatorConnector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            handle.createStatement(StringUtils.format("DROP TABLE %s", tableName))
                  .execute();
            return null;
          }
        }
    );
  }
}
