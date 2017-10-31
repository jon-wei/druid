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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.StringUtils;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.exceptions.CallbackFailedException;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.util.List;
import java.util.Map;

public class SQLBasicAuthorizerStorageConnectorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public final TestDerbyAuthorizerStorageConnector.DerbyConnectorRule authorizerRule =
      new TestDerbyAuthorizerStorageConnector.DerbyConnectorRule("test");

  private TestDerbyAuthorizerStorageConnector authorizerConnector;

  @Before
  public void setUp() throws Exception
  {
    authorizerConnector = authorizerRule.getConnector();
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
    authorizerConnector.getDBI().withHandle(
        new HandleCallback<Void>()
        {
          @Override
          public Void withHandle(Handle handle) throws Exception
          {
            for (String table : authorizerConnector.getTableNames()) {
              Assert.assertTrue(
                  StringUtils.format("authorization table %s was not created!", table),
                  authorizerConnector.tableExists(handle, table)
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
    authorizerConnector.createUser("druid");
    Map<String, Object> expectedUser = ImmutableMap.of(
        "name", "druid"
    );
    Map<String, Object> dbUser = authorizerConnector.getUser("druid");
    Assert.assertEquals(expectedUser, dbUser);

    authorizerConnector.deleteUser("druid");
    dbUser = authorizerConnector.getUser("druid");
    Assert.assertEquals(null, dbUser);
  }

  @Test
  public void testDeleteNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    authorizerConnector.deleteUser("druid");
  }

  @Test
  public void testCreateDuplicateUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] already exists.");
    authorizerConnector.createUser("druid");
    authorizerConnector.createUser("druid");
  }

  // role tests
  @Test
  public void testCreateRole() throws Exception
  {
    authorizerConnector.createRole("druid");
    Map<String, Object> expectedRole = ImmutableMap.of(
        "name", "druid"
    );
    Map<String, Object> dbRole = authorizerConnector.getRole("druid");
    Assert.assertEquals(expectedRole, dbRole);
  }

  @Test
  public void testDeleteNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druid] does not exist.");
    authorizerConnector.deleteRole("druid");
  }

  @Test
  public void testCreateDuplicateRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druid] already exists.");
    authorizerConnector.createRole("druid");
    authorizerConnector.createRole("druid");
  }

  // role and user tests
  @Test
  public void testAddAndRemoveRole() throws Exception
  {
    authorizerConnector.createUser("druid");
    authorizerConnector.createRole("druidRole");
    authorizerConnector.assignRole("druid", "druidRole");

    List<Map<String, Object>> expectedUsersWithRole = ImmutableList.of(
        ImmutableMap.of("name", "druid")
    );

    List<Map<String, Object>> expectedRolesForUser = ImmutableList.of(
        ImmutableMap.of("name", "druidRole")
    );

    List<Map<String, Object>> usersWithRole = authorizerConnector.getUsersWithRole("druidRole");
    List<Map<String, Object>> rolesForUser = authorizerConnector.getRolesForUser("druid");

    Assert.assertEquals(expectedUsersWithRole, usersWithRole);
    Assert.assertEquals(expectedRolesForUser, rolesForUser);

    authorizerConnector.unassignRole("druid", "druidRole");
    usersWithRole = authorizerConnector.getUsersWithRole("druidRole");
    rolesForUser = authorizerConnector.getRolesForUser("druid");

    Assert.assertEquals(ImmutableList.of(), usersWithRole);
    Assert.assertEquals(ImmutableList.of(), rolesForUser);
  }

  @Test
  public void testAddRoleToNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [nonUser] does not exist.");
    authorizerConnector.createRole("druid");
    authorizerConnector.assignRole("nonUser", "druid");
  }

  @Test
  public void testAddNonexistentRoleToUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [nonRole] does not exist.");
    authorizerConnector.createUser("druid");
    authorizerConnector.assignRole("druid", "nonRole");
  }

  @Test
  public void testAddExistingRoleToUserFails() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] already has role [druidRole].");
    authorizerConnector.createUser("druid");
    authorizerConnector.createRole("druidRole");
    authorizerConnector.assignRole("druid", "druidRole");
    authorizerConnector.assignRole("druid", "druidRole");
  }

  @Test
  public void testUnassignInvalidRoleAssignmentFails() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not have role [druidRole].");

    authorizerConnector.createUser("druid");
    authorizerConnector.createRole("druidRole");

    List<Map<String, Object>> usersWithRole = authorizerConnector.getUsersWithRole("druidRole");
    List<Map<String, Object>> rolesForUser = authorizerConnector.getRolesForUser("druid");

    Assert.assertEquals(ImmutableList.of(), usersWithRole);
    Assert.assertEquals(ImmutableList.of(), rolesForUser);

    authorizerConnector.unassignRole("druid", "druidRole");
  }

  // role and permission tests
  @Test
  public void testAddPermissionToRole() throws Exception
  {
    authorizerConnector.createUser("druid");
    authorizerConnector.createRole("druidRole");
    authorizerConnector.assignRole("druid", "druidRole");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    authorizerConnector.addPermission("druidRole", permission);

    List<Map<String, Object>> expectedPerms = ImmutableList.of(
        ImmutableMap.of(
            "id", 1,
            "resourceAction", permission
        )
    );
    List<Map<String, Object>> dbPermsRole = authorizerConnector.getPermissionsForRole("druidRole");
    Assert.assertEquals(expectedPerms, dbPermsRole);
    List<Map<String, Object>> dbPermsUser = authorizerConnector.getPermissionsForUser("druid");
    Assert.assertEquals(expectedPerms, dbPermsUser);

    authorizerConnector.deletePermission(1);
    dbPermsRole = authorizerConnector.getPermissionsForRole("druidRole");
    Assert.assertEquals(ImmutableList.of(), dbPermsRole);
    dbPermsUser = authorizerConnector.getPermissionsForUser("druid");
    Assert.assertEquals(ImmutableList.of(), dbPermsUser);
  }

  @Test
  public void testAddPermissionToNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druidRole] does not exist.");

    ResourceAction permission = new ResourceAction(
        new Resource("testResource", ResourceType.DATASOURCE),
        Action.WRITE
    );
    authorizerConnector.addPermission("druidRole", permission);
  }

  @Test
  public void testAddBadPermission() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Invalid permission, resource name regex[??????????] does not compile.");
    authorizerConnector.createRole("druidRole");
    ResourceAction permission = new ResourceAction(
        new Resource("??????????", ResourceType.DATASOURCE),
        Action.WRITE
    );
    authorizerConnector.addPermission("druidRole", permission);
  }

  @Test
  public void testGetPermissionForNonExistentRole() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("Role [druidRole] does not exist.");
    authorizerConnector.getPermissionsForRole("druidRole");
  }

  @Test
  public void testGetPermissionForNonExistentUser() throws Exception
  {
    expectedException.expect(CallbackFailedException.class);
    expectedException.expectMessage("User [druid] does not exist.");
    authorizerConnector.getPermissionsForUser("druid");
  }

  private void createAllTables()
  {
    authorizerConnector.createUserTable();
    authorizerConnector.createRoleTable();
    authorizerConnector.createPermissionTable();
    authorizerConnector.createUserRoleTable();
  }

  private void dropAllTables()
  {
    for (String table : authorizerConnector.getTableNames()) {
      dropAuthorizerTable(table);
    }
  }

  private void dropAuthorizerTable(final String tableName)
  {
    authorizerConnector.getDBI().withHandle(
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
