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
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscovery;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.ManageLifecycleLast;
import io.druid.guice.annotations.EscalatedClient;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.joda.time.Duration;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@ManageLifecycleLast
public class CoordinatorBasicAuthenticatorCacheNotifier
{
  private static final Logger log = new Logger(CoordinatorBasicAuthenticatorCacheNotifier.class);
  private static final EmittingLogger LOG = new EmittingLogger(CoordinatorBasicAuthenticatorCacheNotifier.class);

  private static final List<String> NODE_TYPES = Arrays.asList(
      DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
      DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
      DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL,
      DruidNodeDiscoveryProvider.NODE_TYPE_PEON,
      DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER,
      DruidNodeDiscoveryProvider.NODE_TYPE_MM
  );

  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final Set<String> authenticatorsToUpdate;

  private volatile ScheduledExecutorService exec;
  private Thread notifierThread;

  @Inject
  public CoordinatorBasicAuthenticatorCacheNotifier(
      DruidNodeDiscoveryProvider discoveryProvider,
      @EscalatedClient HttpClient httpClient,
      final @Smile ObjectMapper smileMapper
  )
  {
    this.discoveryProvider = discoveryProvider;
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.authenticatorsToUpdate = new HashSet<>();
  }

  @LifecycleStart
  public void start()
  {
    this.exec = Execs.scheduledSingleThreaded("CoordinatorBasicAuthenticatorCacheNotifier-Exec--%d");
    notifierThread = Execs.makeThread(
        "CoordinatorBasicAuthenticatorCacheNotifier-notifierThread",
        () -> {
          while (!Thread.interrupted()) {
            try {
              log.info("About to send cache update notification");
              Set<String> authenticatorsToUpdateSnapshot;
              synchronized (authenticatorsToUpdate) {
                if (authenticatorsToUpdate.isEmpty()) {
                  authenticatorsToUpdate.wait();
                }
                log.info("Sending cache update notifications");
                authenticatorsToUpdateSnapshot = new HashSet<String>(authenticatorsToUpdate);
                authenticatorsToUpdate.clear();
              }
              for (String authenticator : authenticatorsToUpdateSnapshot) {
                sendUpdate(authenticator);
              }
            }
            catch (Throwable t) {
              LOG.makeAlert(t, "Error occured while handling updates for cachedUserMaps.").emit();
            }
          }
        },
        true
    );
    notifierThread.start();
  }

  public void addUpdate(String updatedAuthenticatorPrefix)
  {
    synchronized (authenticatorsToUpdate) {
      authenticatorsToUpdate.add(updatedAuthenticatorPrefix);
      authenticatorsToUpdate.notify();
    }
  }

  private void sendUpdate(String updatedAuthenticatorPrefix)
  {
    for (String nodeType : NODE_TYPES) {
      DruidNodeDiscovery nodeDiscovery = discoveryProvider.getForNodeType(nodeType);
      Collection<DiscoveryDruidNode> nodes = nodeDiscovery.getAllNodes();
      for (DiscoveryDruidNode node : nodes) {
        URL listenerURL = getListenerURL(node.getDruidNode(), updatedAuthenticatorPrefix);
        final AtomicInteger returnCode = new AtomicInteger(0);
        final AtomicReference<String> reasonString = new AtomicReference<>(null);
        httpClient.go(
            new Request(HttpMethod.POST, listenerURL),
            makeResponseHandler(returnCode, reasonString),
            Duration.millis(2000)
        );
      }
    }
  }

  private static URL getListenerURL(DruidNode druidNode, String authPrefix)
  {
    try {
      return new URL(
          druidNode.getServiceScheme(),
          druidNode.getHost(),
          druidNode.getPortToUse(),
          StringUtils.format("/druid/security/internal/authentication/listen/%s", authPrefix)
      );
    }
    catch (MalformedURLException mue) {
      log.error("WTF? Malformed url for DruidNode[%s] and authPrefix[%s]", druidNode, authPrefix);
      throw new RuntimeException(mue);
    }
  }

  private static HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
      final AtomicInteger returnCode,
      final AtomicReference<String> reasonString
  )
  {
    return new SequenceInputStreamResponseHandler()
    {
      @Override
      public ClientResponse<InputStream> handleResponse(HttpResponse response)
      {
        returnCode.set(response.getStatus().getCode());
        reasonString.set(response.getStatus().getReasonPhrase());
        return super.handleResponse(response);
      }
    };
  }
}
