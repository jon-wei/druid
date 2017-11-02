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
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.db.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;

import java.io.IOException;
import java.util.Map;

public class BasicAuthenticatorMetadataStorageUpdater
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
  private final int numRetries = 5;

  @Inject
  public BasicAuthenticatorMetadataStorageUpdater(
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
    int attempts = 0;
    while(attempts < numRetries) {
      if (createUserOnce(userName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not create user[%s] due to concurrent update contention.", userName);
  }

  public void deleteUser(String userName)
  {
    int attempts = 0;
    while(attempts < numRetries) {
      if (deleteUserOnce(userName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not delete user[%s] due to concurrent update contention.", userName);
  }

  public void setUserCredentials(String userName, char[] password)
  {
    BasicAuthenticatorCredentials credentials = new BasicAuthenticatorCredentials(password);
    int attempts = 0;
    while(attempts < numRetries) {
      if (setUserCredentialOnce(userName, credentials)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not set credentials for user[%s] due to concurrent update contention.", userName);
  }

  private static String getPrefixedKeyColumn(String keyPrefix, String keyName)
  {
    return StringUtils.format("basic_authentication_%s_%s", keyPrefix, keyName);
  }

  private byte[] getCurrentUserMapBytes()
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        CONFIG_TABLE_KEY_COLUMN,
        CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(authenticatorPrefix, USERS)
    );
  }

  private Map<String, BasicAuthenticatorUser> deserializeUserMap(byte[] userMapBytes)
  {
    Map<String, BasicAuthenticatorUser> userMap;
    if (userMapBytes == null) {
      userMap = Maps.newHashMap();
    } else {
      try {
        userMap = objectMapper.readValue(userMapBytes, USER_MAP_TYPE_REFERENCE);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return userMap;
  }

  private byte[] serializeUserMap(Map<String, BasicAuthenticatorUser> userMap)
  {
    try {
      return objectMapper.writeValueAsBytes(userMap);
    }
    catch (IOException ioe) {
      throw new ISE("WTF? Couldn't serialize userMap!");
    }
  }

  private boolean tryUpdateUserMap(byte[] oldValue, byte[] newValue) {
    try {
      return connector.compareAndSwap(
          connectorConfig.getConfigTable(),
          CONFIG_TABLE_KEY_COLUMN,
          CONFIG_TABLE_VALUE_COLUMN,
          getPrefixedKeyColumn(authenticatorPrefix, USERS),
          oldValue,
          newValue
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private boolean createUserOnce(String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes();
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, null));
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(oldValue, newValue);
  }

  private boolean deleteUserOnce(String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes();
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.remove(userName);
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(oldValue, newValue);
  }

  private boolean setUserCredentialOnce(String userName, BasicAuthenticatorCredentials credentials)
  {
    byte[] oldValue = getCurrentUserMapBytes();
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, credentials));
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(oldValue, newValue);
  }
}
