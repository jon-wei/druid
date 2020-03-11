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

package org.apache.druid.segment.join.filter;

import org.apache.druid.query.filter.Filter;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class JoinFilterPreAnalysis
{
  private final Filter originalFilter;
  private final List<Filter> normalizedBaseTableClauses;
  private final List<Filter> normalizedJoinTableClauses;
  private final Map<String, Optional<JoinFilterColumnCorrelationAnalysis>>  correlationsByColumn;
  private final boolean enableFilterPushDown;
  private final boolean enableFilterRewrite;

  public JoinFilterPreAnalysis(
      final Filter originalFilter,
      final List<Filter> normalizedBaseTableClauses,
      final List<Filter> normalizedJoinTableClauses,
      final Map<String, Optional<JoinFilterColumnCorrelationAnalysis>>  correlationsByColumn,
      final boolean enableFilterPushDown,
      final boolean enableFilterRewrite
  )
  {
    this.originalFilter = originalFilter;
    this.normalizedBaseTableClauses = normalizedBaseTableClauses;
    this.normalizedJoinTableClauses = normalizedJoinTableClauses;
    this.correlationsByColumn = correlationsByColumn;
    this.enableFilterPushDown = enableFilterPushDown;
    this.enableFilterRewrite = enableFilterRewrite;
  }

  public Filter getOriginalFilter()
  {
    return originalFilter;
  }

  public List<Filter> getNormalizedBaseTableClauses()
  {
    return normalizedBaseTableClauses;
  }

  public List<Filter> getNormalizedJoinTableClauses()
  {
    return normalizedJoinTableClauses;
  }

  public Map<String, Optional<JoinFilterColumnCorrelationAnalysis>>  getCorrelationsByColumn()
  {
    return correlationsByColumn;
  }

  public boolean isEnableFilterPushDown()
  {
    return enableFilterPushDown;
  }

  public boolean isEnableFilterRewrite()
  {
    return enableFilterRewrite;
  }
}

