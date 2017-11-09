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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.druid.concurrent.LifecycleLock;
import io.druid.guice.ManageLifecycleLast;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.db.cache.CoordinatorBasicAuthenticatorCacheNotifier;
import io.druid.security.basic.db.entity.BasicAuthenticatorCredentials;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@ManageLifecycleLast
public class CoordinatorBasicAuthenticatorMetadataStorageUpdater implements BasicAuthenticatorMetadataStorageUpdater
{
  private static final Logger log = new Logger(CoordinatorBasicAuthenticatorMetadataStorageUpdater.class);

  public static final String USERS = "users";
  public static final TypeReference USER_MAP_TYPE_REFERENCE = new TypeReference<Map<String, BasicAuthenticatorUser>>()
  {
  };

  private final Injector injector;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;
  private final ObjectMapper objectMapper;
  private final CoordinatorBasicAuthenticatorCacheNotifier cacheNotifier;
  private final int numRetries = 5;

  private final Map<String, Map<String, BasicAuthenticatorUser>> cachedUserMaps;
  private final Map<String, byte[]> cachedSerializedUserMaps;
  private final LifecycleLock lifecycleLock = new LifecycleLock();

  @Inject
  public CoordinatorBasicAuthenticatorMetadataStorageUpdater(
      Injector injector,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig,
      @Smile ObjectMapper objectMapper,
      CoordinatorBasicAuthenticatorCacheNotifier cacheNotifier
  )
  {
    this.injector = injector;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
    this.objectMapper = objectMapper;
    this.cacheNotifier = cacheNotifier;
    this.cachedUserMaps = new HashMap<>();
    this.cachedSerializedUserMaps = new HashMap<>();
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      log.info("STARTING COORDINATOR BASIC AUTH STORAGE UPDATER");
      AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);
      for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
        Authenticator authenticator = entry.getValue();
        if (authenticator instanceof BasicHTTPAuthenticator) {
          String authenticatorName = entry.getKey();
          BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
          BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
          byte[] userMapBytes = getCurrentUserMapBytes(authenticatorName);
          Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(userMapBytes);
          cachedUserMaps.put(authenticatorName, userMap);
          cachedSerializedUserMaps.put(authenticatorName, userMapBytes);

          if (dbConfig.getInitialAdminPassword() != null && !userMap.containsKey("admin")) {
            createUser(authenticatorName, "admin");
            setUserCredentials(authenticatorName, "admin", dbConfig.getInitialAdminPassword().toCharArray());
          }

          if (dbConfig.getInitialInternalClientPassword() != null && !userMap.containsKey("druid_system")) {
            createUser(authenticatorName, "druid_system");
            setUserCredentials(
                authenticatorName,
                "druid_system",
                dbConfig.getInitialInternalClientPassword().toCharArray()
            );
          }
        }
      }
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  public void createUser(String prefix, String userName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    createUserInternal(prefix, userName);
  }

  public void deleteUser(String prefix, String userName)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    deleteUserInternal(prefix, userName);
  }

  public void setUserCredentials(String prefix, String userName, char[] password)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    setUserCredentialsInternal(prefix, userName, password);
  }

  public Map<String, BasicAuthenticatorUser> getCachedUserMap(String prefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    synchronized (cachedUserMaps) {
      return cachedUserMaps.get(prefix);
    }
  }

  public byte[] getCachedSerializedUserMap(String prefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    synchronized (cachedUserMaps) {
      return cachedSerializedUserMaps.get(prefix);
    }
  }

  public byte[] getCurrentUserMapBytes(String prefix)
  {
    return connector.lookup(
        connectorConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        getPrefixedKeyColumn(prefix, USERS)
    );
  }

  public Map<String, BasicAuthenticatorUser> deserializeUserMap(byte[] userMapBytes)
  {
    Map<String, BasicAuthenticatorUser> userMap;
    if (userMapBytes == null) {
      userMap = Maps.newHashMap();
    } else {
      try {
        userMap = objectMapper.readValue(userMapBytes, USER_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
    return userMap;
  }

  public byte[] serializeUserMap(Map<String, BasicAuthenticatorUser> userMap)
  {
    try {
      return objectMapper.writeValueAsBytes(userMap);
    }
    catch (IOException ioe) {
      throw new ISE("WTF? Couldn't serialize userMap!");
    }
  }

  private static String getPrefixedKeyColumn(String keyPrefix, String keyName)
  {
    return StringUtils.format("basic_authentication_%s_%s", keyPrefix, keyName);
  }

  private boolean tryUpdateUserMap(
      String prefix,
      Map<String, BasicAuthenticatorUser> userMap,
      byte[] oldValue,
      byte[] newValue
  )
  {
    try {
      synchronized (cachedUserMaps) {
        boolean succeeded = connector.compareAndSwap(
            connectorConfig.getConfigTable(),
            MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
            MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
            getPrefixedKeyColumn(prefix, USERS),
            oldValue,
            newValue
        );
        if (succeeded) {
          cachedUserMaps.put(prefix, userMap);
          cachedSerializedUserMaps.put(prefix, newValue);
          cacheNotifier.addUpdate(prefix);
          return true;
        } else {
          return false;
        }
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void createUserInternal(String prefix, String userName)
  {

    int attempts = 0;
    while (attempts < numRetries) {
      if (createUserOnce(prefix, userName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not create user[%s] due to concurrent update contention.", userName);
  }

  private void deleteUserInternal(String prefix, String userName)
  {
    int attempts = 0;
    while (attempts < numRetries) {
      if (deleteUserOnce(prefix, userName)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not delete user[%s] due to concurrent update contention.", userName);
  }

  private void setUserCredentialsInternal(String prefix, String userName, char[] password)
  {
    BasicAuthenticatorCredentials credentials = new BasicAuthenticatorCredentials(password);
    int attempts = 0;
    while (attempts < numRetries) {
      if (setUserCredentialOnce(prefix, userName, credentials)) {
        return;
      } else {
        attempts++;
      }
    }
    throw new ISE("Could not set credentials for user[%s] due to concurrent update contention.", userName);
  }

  private boolean createUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) != null) {
      throw new BasicSecurityDBResourceException("User [%s] already exists.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, null));
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean deleteUserOnce(String prefix, String userName)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.remove(userName);
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }

  private boolean setUserCredentialOnce(String prefix, String userName, BasicAuthenticatorCredentials credentials)
  {
    byte[] oldValue = getCurrentUserMapBytes(prefix);
    Map<String, BasicAuthenticatorUser> userMap = deserializeUserMap(oldValue);
    if (userMap.get(userName) == null) {
      throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
    } else {
      userMap.put(userName, new BasicAuthenticatorUser(userName, credentials));
    }
    byte[] newValue = serializeUserMap(userMap);
    return tryUpdateUserMap(prefix, userMap, oldValue, newValue);
  }
}
