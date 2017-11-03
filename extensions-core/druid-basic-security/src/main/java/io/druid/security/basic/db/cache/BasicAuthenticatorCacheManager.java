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

package io.druid.security.basic.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.client.coordinator.Coordinator;
import io.druid.discovery.DruidLeaderClient;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.db.BasicAuthDBConfig;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

@ManageLifecycle
public class BasicAuthenticatorCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(BasicAuthenticatorCacheManager.class);

  private final DruidLeaderClient druidLeaderClient;
  private final ConcurrentHashMap<String, Map<String, BasicAuthenticatorUser>> cachedUserMaps;
  private final List<String> authenticatorPrefixes;
  private final Set<String> authenticatorsToUpdate;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private volatile ScheduledExecutorService exec;
  private Thread cacheUpdateHandlerThread;

  @Inject
  public BasicAuthenticatorCacheManager(
      @Coordinator DruidLeaderClient druidLeaderClient,
      Injector injector,
      @Smile ObjectMapper objectMapper
  )
  {
    this.druidLeaderClient = druidLeaderClient;
    this.injector = injector;
    this.objectMapper = objectMapper;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.authenticatorPrefixes = new ArrayList<>();
    this.authenticatorsToUpdate = new HashSet<>();
  }

  @LifecycleStart
  public void start()
  {
    initUserMaps();

    this.exec = Execs.scheduledSingleThreaded("BasicAuthenticatorCacheManager-Exec--%d");

    ScheduledExecutors.scheduleWithFixedDelay(
        exec,
        new Duration(0),
        new Duration(30000),
        () -> {
          try {
            for (String authenticatorPrefix : authenticatorPrefixes) {
              Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorPrefix);
              cachedUserMaps.put(authenticatorPrefix, userMap);
            }
          }
          catch (Throwable t) {
            LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
          }
        }
    );

    cacheUpdateHandlerThread = Execs.makeThread(
        "BasicAuthenticatorCacheManager-cacheUpdateHandlerThread",
        () -> {
          while (!Thread.interrupted()) {
            try {
              synchronized (authenticatorsToUpdate) {
                for (String authenticatorPrefix : authenticatorsToUpdate) {
                  Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorPrefix);
                  cachedUserMaps.put(authenticatorPrefix, userMap);
                }
                authenticatorsToUpdate.clear();
              }
              wait();
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
            }
          }
        },
        true
    );
    cacheUpdateHandlerThread.start();
  }

  public void addAuthenticatorToUpdate(String authenticatorPrefix)
  {
    synchronized (authenticatorsToUpdate) {
      authenticatorsToUpdate.add(authenticatorPrefix);
    }
    cacheUpdateHandlerThread.notify();
  }

  private Map<String, BasicAuthenticatorUser> fetchUserMapFromCoordinator(String prefix)
  {
    try {
      Request req = druidLeaderClient.makeRequest(
          HttpMethod.GET,
          StringUtils.format("/druid/coordinator/v1/security/authentication/%s/cachedSerializedUserMap", prefix)
      );
      FullResponseHolder responseHolder = druidLeaderClient.go(req);
      ChannelBuffer buf = responseHolder.getResponse().getContent();
      byte[] userMapBytes = buf.array();
      Map<String, BasicAuthenticatorUser> userMap = objectMapper.readValue(
          userMapBytes,
          BasicAuthenticatorMetadataStorageUpdater.USER_MAP_TYPE_REFERENCE
      );
      return userMap;
    }
    catch (Exception ioe) {
      return null;
    }
  }

  ;

  private void initUserMaps()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);
    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
        Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(dbConfig.getDbPrefix());
        cachedUserMaps.put(dbConfig.getDbPrefix(), userMap);
        authenticatorPrefixes.add(dbConfig.getDbPrefix());
      }
    }
  }
}
