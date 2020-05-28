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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public final class DruidClusterBuilder
{
  public static DruidClusterBuilder newBuilder()
  {
    return new DruidClusterBuilder();
  }

  private final Map<String, Iterable<ServerHolder>> realtimes = new HashMap<>();
  private final Map<String, Iterable<ServerHolder>> historicals = new HashMap<>();
  private final Map<String, Iterable<ServerHolder>> brokers = new HashMap<>();

  private DruidClusterBuilder()
  {
  }

  public DruidClusterBuilder addRealtimesTier(String tierName, ServerHolder... realtimes)
  {
    if (this.realtimes.putIfAbsent(tierName, Arrays.asList(realtimes)) != null) {
      throw new IllegalArgumentException("Duplicate tier: " + tierName);
    }
    return this;
  }

  public DruidClusterBuilder addHistoricalTier(String tierName, ServerHolder... historicals)
  {
    if (this.historicals.putIfAbsent(tierName, Arrays.asList(historicals)) != null) {
      throw new IllegalArgumentException("Duplicate tier: " + tierName);
    }
    return this;
  }

  public DruidClusterBuilder addBrokerTier(String tierName, ServerHolder... brokers)
  {
    if (this.brokers.putIfAbsent(tierName, Arrays.asList(brokers)) != null) {
      throw new IllegalArgumentException("Duplicate tier: " + tierName);
    }
    return this;
  }

  public DruidCluster build()
  {
    return DruidCluster.createDruidClusterFromBuilderInTest(realtimes, historicals, brokers);
  }
}
