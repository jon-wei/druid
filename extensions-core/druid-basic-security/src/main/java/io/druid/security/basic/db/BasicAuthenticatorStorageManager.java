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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.security.basic.BasicSecurityDBResourceException;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BasicAuthenticatorStorageManager
{
  public static final String USERS = "users";
  private static final TypeReference USER_MAP_TYPE_REFERENCE = new TypeReference<Map<String, BasicAuthenticatorUser>>()
  {
  };

  public static final String CONFIG_TABLE_KEY_COLUMN = "name";
  public static final String CONFIG_TABLE_VALUE_COLUMN = "payload";

  private final String authenticatorPrefix;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final ObjectMapper objectMapper;

  @Inject
  public BasicAuthenticatorStorageManager(
      String authenticatorPrefix,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      ObjectMapper objectMapper
  )
  {
    this.authenticatorPrefix = authenticatorPrefix;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.objectMapper = objectMapper;
  }

  public void createUser(String userName)
  {
    final String keyColumn = getPrefixedKeyColumn(authenticatorPrefix, USERS);
    byte[] oldValue = connector.lookup(
        connectorConfig.getConfigTable(),
        CONFIG_TABLE_KEY_COLUMN,
        CONFIG_TABLE_VALUE_COLUMN,
        keyColumn
    );

    Map<String, BasicAuthenticatorUser> userMap;
    if (oldValue == null) {
      userMap = Maps.newHashMap();
    } else {
      try {
        userMap = objectMapper.readValue(oldValue, USER_MAP_TYPE_REFERENCE);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    }

    connector.compareAndSwap(

    )


  }

  private Map<String, BasicAuthenticatorUser> getUserMapAndUpdate

  void deleteUser(String dbPrefix, String userName);

  List<Map<String, Object>> getAllUsers(String dbPrefix);

  Map<String, Object> getUser(String dbPrefix, String userName);

  void setUserCredentials(String dbPrefix, String userName, char[] password);

  boolean checkCredentials(String dbPrefix, String userName, char[] password);

  Map<String, Object> getUserCredentials(String dbPrefix, String userName);

  void createUserTable(String dbPrefix);

  void createUserCredentialsTable(String dbPrefix);

  private static String getPrefixedKeyColumn(String keyPrefix, String keyName)
  {
    return StringUtils.format("basic_authentication_%s_%s", keyPrefix, keyName);
  }

  private Pair<byte[], Map<String, BasicAuthenticatorUser>> getCurrentUserMap()
  {
    final String keyColumn = getPrefixedKeyColumn(authenticatorPrefix, USERS);
    byte[] oldValue = connector.lookup(
        connectorConfig.getConfigTable(),
        CONFIG_TABLE_KEY_COLUMN,
        CONFIG_TABLE_VALUE_COLUMN,
        keyColumn
    );

    Map<String, BasicAuthenticatorUser> userMap;
    if (oldValue == null) {
      userMap = Maps.newHashMap();
    } else {
      try {
        userMap = objectMapper.readValue(oldValue, USER_MAP_TYPE_REFERENCE);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }

    return new Pair<>(oldValue, userMap);
  }
}
