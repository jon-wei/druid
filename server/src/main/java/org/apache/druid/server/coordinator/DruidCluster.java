/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Contains a representation of the current state of the cluster by tier.
 * Each tier is mapped to the list of servers for that tier sorted by available space.
 */
public class DruidCluster
{
  /** This static factory method must be called only from inside DruidClusterBuilder in tests. */
  @VisibleForTesting
  static DruidCluster createDruidClusterFromBuilderInTest(
      Map<String, Iterable<ServerHolder>> realtimes,
      Map<String, Iterable<ServerHolder>> historicals,
      Map<String, Iterable<ServerHolder>> brokers
  )
  {
    return new DruidCluster(realtimes, historicals, brokers);
  }

  private final Map<String, NavigableSet<ServerHolder>> realtimes;
  private final Map<String, NavigableSet<ServerHolder>> historicals;
  private final Map<String, NavigableSet<ServerHolder>> brokers;

  public DruidCluster()
  {
    this.realtimes = new HashMap<>();
    this.historicals = new HashMap<>();
    this.brokers = new HashMap<>();
  }

  private DruidCluster(
      Map<String, Iterable<ServerHolder>> realtimes,
      Map<String, Iterable<ServerHolder>> historicals,
      Map<String, Iterable<ServerHolder>> brokers
  )
  {
    this.realtimes = initializeServerMap(realtimes);
    this.historicals = initializeServerMap(historicals);
    this.brokers = initializeServerMap(brokers);
  }

  private Map<String, NavigableSet<ServerHolder>> initializeServerMap(
      Map<String, Iterable<ServerHolder>> baseMap
  )
  {
    return CollectionUtils.mapValues(
        baseMap,
        holders -> CollectionUtils.newTreeSet(Comparator.reverseOrder(), holders)
    );
  }

  public void add(ServerHolder serverHolder)
  {
    switch (serverHolder.getServer().getType()) {
      case HISTORICAL:
        addHistorical(serverHolder);
        break;
      case REALTIME:
        addRealtime(serverHolder);
        break;
      case BRIDGE:
        addHistorical(serverHolder);
        break;
      case INDEXER_EXECUTOR:
        addRealtime(serverHolder);
        break;
      case BROKER:
        addBroker(serverHolder);
        break;
      default:
        throw new IAE("unknown server type[%s]", serverHolder.getServer().getType());
    }
  }

  private void addRealtime(ServerHolder serverHolder)
  {
    addServer(serverHolder, realtimes);
  }

  private void addHistorical(ServerHolder serverHolder)
  {
    addServer(serverHolder, historicals);
  }

  private void addBroker(ServerHolder serverHolder)
  {
    addServer(serverHolder, brokers);
  }

  private void addServer(ServerHolder serverHolder, Map<String, NavigableSet<ServerHolder>> mapForServerType)
  {
    final ImmutableDruidServer server = serverHolder.getServer();
    final NavigableSet<ServerHolder> tierServers = mapForServerType.computeIfAbsent(
        server.getTier(),
        k -> new TreeSet<>(Collections.reverseOrder())
    );
    tierServers.add(serverHolder);
  }

  public Map<String, NavigableSet<ServerHolder>> getRealtimes()
  {
    return realtimes;
  }

  public Map<String, NavigableSet<ServerHolder>> getHistoricals()
  {
    return historicals;
  }

  public Map<String, NavigableSet<ServerHolder>> getBrokers()
  {
    return brokers;
  }

  public Map<String, NavigableSet<ServerHolder>> getAllServersByTier()
  {
    Map<String, NavigableSet<ServerHolder>> allServersByTier = new HashMap<>();
    addServersByTierForType(allServersByTier, historicals);
    addServersByTierForType(allServersByTier, brokers);
    addServersByTierForType(allServersByTier, realtimes);
    return allServersByTier;
  }

  public void addServersByTierForType(
      Map<String, NavigableSet<ServerHolder>> allServersByTier,
      Map<String, NavigableSet<ServerHolder>> mapForServerType
  )
  {
    for (Map.Entry<String, NavigableSet<ServerHolder>> entry : mapForServerType.entrySet()) {
      NavigableSet<ServerHolder> serversForTier = allServersByTier.computeIfAbsent(
          entry.getKey(),
          (key) -> {
            return CollectionUtils.newTreeSet(Comparator.reverseOrder(), Collections.EMPTY_SET);
          }
      );
      serversForTier.addAll(entry.getValue());
    }
  }

  public Iterable<String> getTierNames()
  {
    Set<String> tierNames = new HashSet<>(historicals.keySet());
    tierNames.addAll(brokers.keySet());
    tierNames.addAll(realtimes.keySet());
    return tierNames;
  }

  public NavigableSet<ServerHolder> getHistoricalsByTier(String tier)
  {
    return historicals.get(tier);
  }

  public NavigableSet<ServerHolder> getBrokersByTier(String tier)
  {
    return brokers.get(tier);
  }

  public NavigableSet<ServerHolder> getRealtimesByTier(String tier)
  {
    return realtimes.get(tier);
  }

  public Collection<ServerHolder> getAllServers()
  {
    final int historicalSize = historicals.values().stream().mapToInt(Collection::size).sum();
    final int brokerSize = brokers.values().stream().mapToInt(Collection::size).sum();
    final int realtimeSize = realtimes.values().stream().mapToInt(Collection::size).sum();
    final List<ServerHolder> allServers = new ArrayList<>(historicalSize + realtimeSize + brokerSize);

    historicals.values().forEach(allServers::addAll);
    brokers.values().forEach(allServers::addAll);
    realtimes.values().forEach(allServers::addAll);
    return allServers;
  }

  public Iterable<NavigableSet<ServerHolder>> getSortedHistoricalsByTier()
  {
    return historicals.values();
  }

  public Iterable<NavigableSet<ServerHolder>> getSortedBrokersByTier()
  {
    return brokers.values();
  }

  public Iterable<NavigableSet<ServerHolder>> getSortedRealtimesByTier()
  {
    return realtimes.values();
  }

  public boolean isEmpty()
  {
    return historicals.isEmpty() && realtimes.isEmpty() && brokers.isEmpty();
  }

  public boolean hasHistoricals()
  {
    return !historicals.isEmpty();
  }

  public boolean hasRealtimes()
  {
    return !realtimes.isEmpty();
  }

  public boolean hasBrokers()
  {
    return !brokers.isEmpty();
  }

  public boolean hasTier(String tier)
  {
    NavigableSet<ServerHolder> historicalServers = historicals.get(tier);
    boolean hasTier = (historicalServers != null) && !historicalServers.isEmpty();
    if (hasTier) {
      return true;
    }

    NavigableSet<ServerHolder> brokerServers = brokers.get(tier);
    hasTier = (brokerServers != null) && !brokerServers.isEmpty();
    if (hasTier) {
      return true;
    }

    NavigableSet<ServerHolder> realtimeServers = realtimes.get(tier);
    hasTier = (realtimeServers != null) && !realtimeServers.isEmpty();
    if (hasTier) {
      return true;
    }

    return false;
  }
}
