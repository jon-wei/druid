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

package io.druid.security.basic.authentication.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.client.coordinator.Coordinator;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidLeaderClient;
import io.druid.guice.ManageLifecycleLast;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.authentication.BasicHTTPAuthenticator;
import io.druid.security.basic.authentication.db.BasicAuthDBConfig;
import io.druid.security.basic.authentication.db.entity.BasicAuthenticatorUser;
import io.druid.security.basic.authentication.db.updater.CoordinatorBasicAuthenticatorMetadataStorageUpdater;
import io.druid.server.security.Authenticator;
import io.druid.server.security.AuthenticatorMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@ManageLifecycleLast
public class DefaultBasicAuthenticatorCacheManager implements BasicAuthenticatorCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(DefaultBasicAuthenticatorCacheManager.class);
  private static final Logger log = new Logger(DefaultBasicAuthenticatorCacheManager.class);

  private final ConcurrentHashMap<String, Map<String, BasicAuthenticatorUser>> cachedUserMaps;
  private final Set<String> authenticatorPrefixes;
  private final Set<String> authenticatorsToUpdate;
  private final Injector injector;
  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;

  private volatile ScheduledExecutorService exec;
  private Thread cacheUpdateHandlerThread;

  @Inject
  public DefaultBasicAuthenticatorCacheManager(
      Injector injector,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.injector = injector;
    this.objectMapper = objectMapper;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.authenticatorPrefixes = new HashSet<>();
    this.authenticatorsToUpdate = new HashSet<>();
    this.druidLeaderClient = druidLeaderClient;

    log.info("created DEFAULT basic auth cache manager.");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      initUserMaps();

      this.exec = Execs.scheduledSingleThreaded("BasicAuthenticatorCacheManager-Exec--%d");

      ScheduledExecutors.scheduleWithFixedDelay(
          exec,
          new Duration(0),
          new Duration(30000),
          () -> {
            try {
              log.info("Scheduled cache poll is running");
              for (String authenticatorPrefix : authenticatorPrefixes) {
                Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorPrefix, false);
                if (userMap != null) {
                  cachedUserMaps.put(authenticatorPrefix, userMap);
                }
              }
              log.info("Scheduled cache poll is done");
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
                log.info("About to handle cache update");
                synchronized (authenticatorsToUpdate) {
                  log.info("Handling cache update");
                  for (String authenticatorPrefix : authenticatorsToUpdate) {
                    Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(
                        authenticatorPrefix,
                        false
                    );
                    if (userMap != null) {
                      cachedUserMaps.put(authenticatorPrefix, userMap);
                    }
                  }
                  authenticatorsToUpdate.clear();
                  log.info("Handled cache update");
                  authenticatorsToUpdate.wait();
                }
              }
              catch (Throwable t) {
                LOG.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
              }
            }
          },
          true
      );
      cacheUpdateHandlerThread.start();
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void addAuthenticatorToUpdate(String authenticatorPrefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    synchronized (authenticatorsToUpdate) {
      authenticatorsToUpdate.add(authenticatorPrefix);
      authenticatorsToUpdate.notify();
    }
  }

  @Override
  public Map<String, BasicAuthenticatorUser> getUserMap(String authenticatorPrefix)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    return cachedUserMaps.get(authenticatorPrefix);
  }

  private Map<String, BasicAuthenticatorUser> fetchUserMapFromCoordinator(String prefix, boolean throwOnFailure)
  {
    try {
      return RetryUtils.retry(
          () -> {
            return tryFetchUserMapFromCoordinator(prefix);
          },
          e -> true,
          10
      );
    }
    catch (Exception e) {
      log.error(e, "Encountered exception while fetching user map for authenticator [%s]", prefix);
      if (throwOnFailure) {
        throw new RuntimeException(e);
      } else {
        return null;
      }
    }
  }

  private Map<String, BasicAuthenticatorUser> tryFetchUserMapFromCoordinator(String prefix) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid/basic-security/authentication/%s/cachedSerializedUserMap", prefix)
    );
    FullResponseHolder responseHolder = druidLeaderClient.go(req);
    ChannelBuffer buf = responseHolder.getResponse().getContent();
    byte[] userMapBytes = buf.array();
    Map<String, BasicAuthenticatorUser> userMap = objectMapper.readValue(
        userMapBytes,
        CoordinatorBasicAuthenticatorMetadataStorageUpdater.USER_MAP_TYPE_REFERENCE
    );
    return userMap;
  }

  private void initUserMaps()
  {
    AuthenticatorMapper authenticatorMapper = injector.getInstance(AuthenticatorMapper.class);
    for (Map.Entry<String, Authenticator> entry : authenticatorMapper.getAuthenticatorMap().entrySet()) {
      Authenticator authenticator = entry.getValue();
      if (authenticator instanceof BasicHTTPAuthenticator) {
        String authenticatorName = entry.getKey();
        BasicHTTPAuthenticator basicHTTPAuthenticator = (BasicHTTPAuthenticator) authenticator;
        BasicAuthDBConfig dbConfig = basicHTTPAuthenticator.getDbConfig();
        Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorName, true);
        cachedUserMaps.put(authenticatorName, userMap);
        authenticatorPrefixes.add(authenticatorName);
      }
    }
  }
}
