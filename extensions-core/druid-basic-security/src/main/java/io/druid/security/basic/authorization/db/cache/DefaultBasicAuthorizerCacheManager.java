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

package io.druid.security.basic.authorization.db.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.client.coordinator.Coordinator;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidLeaderClient;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.concurrent.ScheduledExecutors;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.security.basic.authentication.db.BasicAuthenticatorCommonCacheConfig;
import io.druid.security.basic.authentication.db.entity.BasicAuthenticatorUser;
import io.druid.security.basic.authorization.db.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.db.entity.BasicAuthorizerUser;
import io.druid.server.security.AuthorizerMapper;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

@ManageLifecycle
public class DefaultBasicAuthorizerCacheManager implements BasicAuthorizerCacheManager
{
  private static final EmittingLogger LOG = new EmittingLogger(DefaultBasicAuthorizerCacheManager.class);

  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerUser>> cachedUserMaps;
  private final ConcurrentHashMap<String, Map<String, BasicAuthorizerRole>> cachedRoleMaps;

  private final Set<String> authorizerPrefixes;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper objectMapper;
  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidLeaderClient druidLeaderClient;
  private final BasicAuthenticatorCommonCacheConfig commonCacheConfig;

  private volatile ScheduledExecutorService exec;

  @Inject
  public DefaultBasicAuthorizerCacheManager(
      AuthorizerMapper authorizerMapper,
      BasicAuthenticatorCommonCacheConfig commonCacheConfig,
      @Smile ObjectMapper objectMapper,
      @Coordinator DruidLeaderClient druidLeaderClient
  )
  {
    this.authorizerMapper = authorizerMapper;
    this.commonCacheConfig = commonCacheConfig;
    this.objectMapper = objectMapper;
    this.cachedUserMaps = new ConcurrentHashMap<>();
    this.authorizerPrefixes = new HashSet<>();
    this.druidLeaderClient = druidLeaderClient;

    LOG.info("created DEFAULT basic auth cache manager.");
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
          new Duration(commonCacheConfig.getPollingPeriod()),
          new Duration(commonCacheConfig.getPollingPeriod()),
          () -> {
            try {
              LOG.info("Scheduled cache poll is running");
              for (String authenticatorPrefix : authenticatorPrefixes) {
                Map<String, BasicAuthenticatorUser> userMap = fetchUserMapFromCoordinator(authenticatorPrefix, false);
                if (userMap != null) {
                  cachedUserMaps.put(authenticatorPrefix, userMap);
                }
              }
              LOG.info("Scheduled cache poll is done");

              long randomDelay = ThreadLocalRandom.current().nextLong(0, commonCacheConfig.getMaxRandomDelay());
              LOG.info("Inserting random polling delay of [%s] ms", randomDelay);
              Thread.sleep(randomDelay);
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while polling for cachedUserMaps.").emit();
            }
          }
      );

      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void handleAuthorizerUpdate(String authorizerPrefix, byte[] serializedUserMap, byte[] serializedRoleMap)
  {

  }

  @Override
  public Map<String, BasicAuthorizerUser> getUserMap(String authorizerPrefix)
  {
    return null;
  }

  @Override
  public Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix)
  {
    return null;
  }


  private Map<String, BasicAuthorizerUser> fetchUserMapFromCoordinator(String prefix, boolean throwOnFailure)
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
      LOG.makeAlert(e, "Encountered exception while fetching user map for authorizer [%s]", prefix);
      if (throwOnFailure) {
        throw new RuntimeException(e);
      } else {
        return null;
      }
    }
  }

  private Map<String, BasicAuthorizerUser> tryFetchUserMapFromCoordinator(String prefix) throws Exception
  {
    Request req = druidLeaderClient.makeRequest(
        HttpMethod.GET,
        StringUtils.format("/druid-ext/basic-security/authentication/%s/cachedSerializedUserMap", prefix)
    );
    FullResponseHolder responseHolder = druidLeaderClient.go(req);
    ChannelBuffer buf = responseHolder.getResponse().getContent();
    byte[] userMapBytes = buf.array();
    Map<String, BasicAuthorizerUser> userMap = objectMapper.readValue(
        userMapBytes,
        CoordinatorBasicAuthorizerMetadataStorageUpdater.USER_MAP_TYPE_REFERENCE
    );
    return userMap;
  }

  private void initUserMaps()
  {
    for (Map.Entry<String, Authorizer> entry : authorizerMapper.getAuthorizerMap().entrySet()) {
      Authorizer authorizer = entry.getValue();
      if (authorizer instanceof BasicHTTPAuthorizer) {
        String authorizerName = entry.getKey();
        BasicHTTPAuthorizer basicHTTPAuthorizer = (BasicHTTPAuthorizer) authorizer;
        Map<String, BasicAuthorizerUser> userMap = fetchUserMapFromCoordinator(authorizerName, true);
        cachedUserMaps.put(authorizerName, userMap);
        authorizerPrefixes.add(authorizerName);
      }
    }
  }
}
