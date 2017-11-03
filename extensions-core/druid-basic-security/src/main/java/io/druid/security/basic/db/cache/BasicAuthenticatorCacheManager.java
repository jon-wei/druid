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
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.db.BasicAuthDBConfig;
import io.druid.security.basic.db.BasicAuthenticatorMetadataStorageUpdater;
import io.druid.security.basic.db.entity.BasicAuthenticatorUser;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.eclipse.jetty.util.ConcurrentArrayQueue;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

@ManageLifecycle
public class BasicAuthenticatorCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(BasicAuthenticatorCacheManager.class);

  private final DruidLeaderClient druidLeaderClient;
  private final ConcurrentHashMap<String, Map<String, BasicAuthenticatorUser>> userMaps;
  private final List<String> authenticatorPrefixes;
  private final Queue<String> authenticatorsToUpdate;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private Thread cachePollingThread;
  private Thread cacheUpdateListenerThread;

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
    this.userMaps = new ConcurrentHashMap<>();
    this.authenticatorPrefixes = new ArrayList<>();
    this.authenticatorsToUpdate = new ConcurrentArrayQueue<>();
  }

  private Map<String, BasicAuthenticatorUser> getUserMap(String prefix)
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
    } catch (Exception ioe) {
      return null;
    }
  };

  private void initUserMaps()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);
    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
        Map<String, BasicAuthenticatorUser> userMap = getUserMap(dbConfig.getDbPrefix());
        userMaps.put(dbConfig.getDbPrefix(), userMap);
        authenticatorPrefixes.add(dbConfig.getDbPrefix());
      }
    }
  }

  @LifecycleStart
  public void start()
  {
    initUserMaps();
    cachePollingThread = Execs.makeThread(
        "BasicAuthenticatorCacheManager-cachePollingThread",
        () -> {
          while (!Thread.interrupted()) {
            try {
              for (String authenticatorPrefix : authenticatorPrefixes) {
                Map<String, BasicAuthenticatorUser> userMap = getUserMap(authenticatorPrefix);
                userMaps.put(authenticatorPrefix, userMap);
              }
              Thread.sleep(30000);
            } catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for userMaps.").emit();
            }
          }
        },
        true
    );
    cacheUpdateListenerThread = Execs.makeThread(
        "BasicAuthenticatorCacheManager-cachePollingThread",
        () -> {
          while (!Thread.interrupted()) {
            try {
              for (String authenticatorPrefix : authenticatorPrefixes) {
                Map<String, BasicAuthenticatorUser> userMap = getUserMap(authenticatorPrefix);
                userMaps.put(authenticatorPrefix, userMap);
              }
              Thread.sleep(30000);
            } catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for userMaps.").emit();
            }
          }
        },
        true
    );
  }
}
