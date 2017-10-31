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

package io.druid.security.basic.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.metadata.BaseSQLMetadataConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.server.security.Action;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import org.apache.commons.dbcp2.BasicDataSource;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.ResultSetMapper;
import org.skife.jdbi.v2.util.IntegerMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public abstract class SQLBasicAuthorizerStorageConnector
    extends BaseSQLMetadataConnector
    implements BasicAuthorizerStorageConnector
{
  public static final String USERS = "users";
  public static final String PERMISSIONS = "permissions";
  public static final String ROLES = "roles";
  public static final String USER_ROLES = "user_roles";

  private static final String DEFAULT_ADMIN_NAME = "admin";
  private static final String DEFAULT_ADMIN_ROLE = "admin";

  private static final String DEFAULT_SYSTEM_USER_NAME = "druid_system";
  private static final String DEFAULT_SYSTEM_USER_ROLE = "druid_system";

  private final Supplier<MetadataStorageConnectorConfig> config;
  private final Supplier<BasicAuthDBConfig> dbConfigSupplier;
  private final ObjectMapper jsonMapper;
  private final PermissionsMapper permMapper;

  protected final String userTableName;
  protected final String roleTableName;
  protected final String permissionTableName;
  protected final String userRoleTableName;
  protected final List<String> tableNames;

  @Inject
  public SQLBasicAuthorizerStorageConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<BasicAuthDBConfig> dbConfigSupplier,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.dbConfigSupplier = dbConfigSupplier;
    this.jsonMapper = jsonMapper;
    this.permMapper = new PermissionsMapper();
    this.shouldRetry = new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        return isTransientException(e);
      }
    };

    this.userTableName = getPrefixedTableName(USERS);
    this.roleTableName = getPrefixedTableName(ROLES);
    this.permissionTableName = getPrefixedTableName(PERMISSIONS);
    this.userRoleTableName = getPrefixedTableName(USER_ROLES);
    this.tableNames = ImmutableList.of(
        userRoleTableName,
        permissionTableName,
        roleTableName,
        userTableName
    );
  }

  @LifecycleStart
  public void start()
  {
    createUserTable();
    createRoleTable();
    createPermissionTable();
    createUserRoleTable();

    makeDefaultSuperuser(DEFAULT_ADMIN_NAME, DEFAULT_ADMIN_ROLE);
    makeDefaultSuperuser(DEFAULT_SYSTEM_USER_NAME, DEFAULT_SYSTEM_USER_ROLE);
  }

  @Override
  public void createRoleTable()
  {
    createTable(
        roleTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                roleTableName
            )
        )
    );
  }

  @Override
  public void createUserTable()
  {
    createTable(
        userTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  name VARCHAR(255) NOT NULL,\n"
                + "  PRIMARY KEY (name),\n"
                + "  UNIQUE (name)\n"
                + ")",
                userTableName
            )
        )
    );
  }

  @Override
  public void createPermissionTable()
  {
    createTable(
        permissionTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  id INTEGER NOT NULL,\n"
                + "  resource_json VARCHAR(255) NOT NULL,\n"
                + "  role_name INTEGER NOT NULL, \n"
                + "  PRIMARY KEY (id),\n"
                + "  FOREIGN KEY (role_name) REFERENCES %2$s(name) ON DELETE CASCADE\n"
                + ")",
                permissionTableName,
                roleTableName
            )
        )
    );
  }

  @Override
  public void createUserRoleTable()
  {
    createTable(
        userRoleTableName,
        ImmutableList.of(
            StringUtils.format(
                "CREATE TABLE %1$s (\n"
                + "  user_name VARCHAR(255) NOT NULL,\n"
                + "  role_name VARCHAR(255) NOT NULL, \n"
                + "  FOREIGN KEY (user_name) REFERENCES %2$s(name) ON DELETE CASCADE,\n"
                + "  FOREIGN KEY (role_name) REFERENCES %3$s(name) ON DELETE CASCADE\n"
                + ")",
                userRoleTableName,
                userTableName,
                roleTableName
            )
        )
    );
  }

  @Override
  public void createUser(String userName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCount(handle, userName);
            if (count != 0) {
              throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
            }

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name) VALUES (:user_name)", userTableName
                )
            )
                  .bind("user_name", userName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void deleteUser(String userName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getUserCount(handle, userName);
            if (count == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE name = :userName", userTableName
                )
            )
                  .bind("userName", userName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void createRole(String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getRoleCount(handle, roleName);
            if (count != 0) {
              throw new BasicSecurityDBResourceException("Role [%s] already exists.", roleName);
            }
            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (name) VALUES (:roleName)", roleTableName
                )
            )
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void deleteRole(String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int count = getRoleCount(handle, roleName);
            if (count == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE name = :roleName", roleTableName
                )
            )
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void addPermission(String roleName, ResourceAction resourceAction)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int roleCount = getRoleCount(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            // make sure the resource regex compiles
            try {
              Pattern pattern = Pattern.compile(resourceAction.getResource().getName());
            }
            catch (PatternSyntaxException pse) {
              throw new BasicSecurityDBResourceException(
                  "Invalid permission, resource name regex[%s] does not compile.",
                  resourceAction.getResource().getName()
              );
            }

            try {
              byte[] serializedResourceAction = jsonMapper.writeValueAsBytes(resourceAction);
              handle.createStatement(
                  StringUtils.format(
                      "INSERT INTO %1$s (resource_json, role_name) VALUES (:resourceJson, :roleName)",
                      permissionTableName
                  )
              )
                    .bind("resourceJson", serializedResourceAction)
                    .bind("roleName", roleName)
                    .execute();

              return null;
            }
            catch (JsonProcessingException jpe) {
              throw new IAE(jpe, "Could not serialize resourceAction [%s].", resourceAction);
            }
          }
        }
    );
  }

  @Override
  public void deletePermission(int permissionId)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int permCount = getPermissionCount(handle, permissionId);
            if (permCount == 0) {
              throw new BasicSecurityDBResourceException("Permission with id [%s] does not exist.", permissionId);
            }
            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE id = :permissionId", permissionTableName
                )
            )
                  .bind("permissionId", permissionId)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void assignRole(String userName, String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCount(handle, userName);
            int roleCount = getRoleCount(handle, roleName);

            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            int userRoleMappingCount = getUserRoleMappingCount(handle, userName, roleName);
            if (userRoleMappingCount != 0) {
              throw new BasicSecurityDBResourceException("User [%s] already has role [%s].", userName, roleName);
            }

            handle.createStatement(
                StringUtils.format(
                    "INSERT INTO %1$s (user_name, role_name) VALUES (:userName, :roleName)", userRoleTableName
                )
            )
                  .bind("userName", userName)
                  .bind("roleName", roleName)
                  .execute();
            return null;
          }
        }
    );
  }

  @Override
  public void unassignRole(String userName, String roleName)
  {
    getDBI().inTransaction(
        new TransactionCallback<Void>()
        {
          @Override
          public Void inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            int userCount = getUserCount(handle, userName);
            int roleCount = getRoleCount(handle, roleName);

            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            int userRoleMappingCount = getUserRoleMappingCount(handle, userName, roleName);
            if (userRoleMappingCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not have role [%s].", userName, roleName);
            }

            handle.createStatement(
                StringUtils.format(
                    "DELETE FROM %1$s WHERE user_name = :userName AND role_name = :roleName", userRoleTableName
                )
            )
                  .bind("userName", userName)
                  .bind("roleName", roleName)
                  .execute();

            return null;
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getAllUsers()
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s", userTableName)
                )
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getAllRoles()
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s", roleTableName)
                )
                .list();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s where name = :userName", userTableName)
                )
                .bind("userName", userName)
                .first();
          }
        }
    );
  }

  @Override
  public Map<String, Object> getRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<Map<String, Object>>()
        {
          @Override
          public Map<String, Object> inTransaction(Handle handle, TransactionStatus transactionStatus) throws Exception
          {
            return handle
                .createQuery(
                    StringUtils.format("SELECT * FROM %1$s where name = :roleName", roleTableName)
                )
                .bind("roleName", roleName)
                .first();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getRolesForUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int userCount = getUserCount(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT %1$s.name\n"
                        + "FROM %1$s\n"
                        + "JOIN %2$s\n"
                        + "    ON %2$s.role_name = %1$s.name\n"
                        + "WHERE %2$s.user_name = :userName",
                        roleTableName,
                        userRoleTableName
                    )
                )
                .bind("userName", userName)
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getUsersWithRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int roleCount = getRoleCount(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT %1$s.name\n"
                        + "FROM %1$s\n"
                        + "JOIN %2$s\n"
                        + "    ON %2$s.user_name = %1$s.name\n"
                        + "WHERE %2$s.role_name = :roleName",
                        userTableName,
                        userRoleTableName
                    )
                )
                .bind("roleName", roleName)
                .list();
          }
        }
    );
  }

  public List<String> getTableNames()
  {
    return tableNames;
  }

  private class PermissionsMapper implements ResultSetMapper<Map<String, Object>>
  {
    @Override
    public Map<String, Object> map(int index, ResultSet resultSet, StatementContext context)
        throws SQLException
    {
      int id = resultSet.getInt("id");
      byte[] resourceJson = resultSet.getBytes("resource_json");
      try {
        final ResourceAction resourceAction = jsonMapper.readValue(resourceJson, ResourceAction.class);
        return ImmutableMap.of(
            "id", id,
            "resourceAction", resourceAction
        );
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public List<Map<String, Object>> getPermissionsForRole(String roleName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int roleCount = getRoleCount(handle, roleName);
            if (roleCount == 0) {
              throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT %1$s.id, %1$s.resource_json\n"
                        + "FROM %1$s\n"
                        + "WHERE %1$s.role_name = :roleName",
                        permissionTableName
                    )
                )
                .map(permMapper)
                .bind("roleName", roleName)
                .list();
          }
        }
    );
  }

  @Override
  public List<Map<String, Object>> getPermissionsForUser(String userName)
  {
    return getDBI().inTransaction(
        new TransactionCallback<List<Map<String, Object>>>()
        {
          @Override
          public List<Map<String, Object>> inTransaction(Handle handle, TransactionStatus transactionStatus)
              throws Exception
          {
            int userCount = getUserCount(handle, userName);
            if (userCount == 0) {
              throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
            }

            return handle
                .createQuery(
                    StringUtils.format(
                        "SELECT %1$s.id, %1$s.resource_json, %2$s.name\n"
                        + "FROM %1$s\n"
                        + "JOIN %2$s\n"
                        + "    ON %1$s.role_name = %2$s.name\n"
                        + "JOIN %3$s\n"
                        + "    ON %3$s.role_name = %2$s.name\n"
                        + "WHERE %3$s.user_name = :userName",
                        permissionTableName,
                        roleTableName,
                        userRoleTableName
                    )
                )
                .map(permMapper)
                .bind("userName", userName)
                .list();
          }
        }
    );
  }

  public MetadataStorageConnectorConfig getConfig()
  {
    return config.get();
  }

  public String getValidationQuery()
  {
    return "SELECT 1";
  }

  protected BasicDataSource getDatasource()
  {
    MetadataStorageConnectorConfig connectorConfig = getConfig();

    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setUsername(connectorConfig.getUser());
    dataSource.setPassword(connectorConfig.getPassword());
    String uri = connectorConfig.getConnectURI();
    dataSource.setUrl(uri);

    dataSource.setValidationQuery(getValidationQuery());
    dataSource.setTestOnBorrow(true);

    return dataSource;
  }

  protected String getPrefixedTableName(String baseTableName)
  {
    return StringUtils.format("basic_authorization_%s_%s", dbConfigSupplier.get().getDbPrefix(), baseTableName);
  }

  private int getUserCount(Handle handle, String userName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", userTableName, "name")
        )
        .bind("key", userName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getRoleCount(Handle handle, String roleName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", roleTableName, "name")
        )
        .bind("key", roleName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getPermissionCount(Handle handle, int permissionId)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :key", permissionTableName, "id")
        )
        .bind("key", permissionId)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private int getUserRoleMappingCount(Handle handle, String userName, String roleName)
  {
    return handle
        .createQuery(
            StringUtils.format("SELECT COUNT(*) FROM %1$s WHERE %2$s = :userkey AND %3$s = :rolekey",
                               userRoleTableName,
                               "user_name",
                               "role_name"
            )
        )
        .bind("userkey", userName)
        .bind("rolekey", roleName)
        .map(IntegerMapper.FIRST)
        .first();
  }

  private void makeDefaultSuperuser(String username, String role)
  {
    if (getUser(username) != null) {
      return;
    }

    createUser(username);
    createRole(role);
    assignRole(username, role);

    ResourceAction datasourceR = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.READ
    );

    ResourceAction datasourceW = new ResourceAction(
        new Resource(".*", ResourceType.DATASOURCE),
        Action.WRITE
    );

    ResourceAction configR = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.READ
    );

    ResourceAction configW = new ResourceAction(
        new Resource(".*", ResourceType.CONFIG),
        Action.WRITE
    );

    ResourceAction stateR = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.READ
    );

    ResourceAction stateW = new ResourceAction(
        new Resource(".*", ResourceType.STATE),
        Action.WRITE
    );

    List<ResourceAction> resActs = Lists.newArrayList(datasourceR, datasourceW, configR, configW, stateR, stateW);

    for (ResourceAction resAct : resActs) {
      addPermission(role, resAct);
    }
  }
}
