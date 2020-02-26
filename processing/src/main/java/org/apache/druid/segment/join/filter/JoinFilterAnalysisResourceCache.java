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

import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.join.JoinableClause;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A cache for holding cross-segment shared resources used by join filter analysis
 */
public class JoinFilterAnalysisResourceCache
{
  private volatile Filter normalizedFilter = null;

  private final ConcurrentHashMap<String, Optional<List<JoinFilterColumnCorrelationAnalysis>>> correlationAnalysisCache =
      new ConcurrentHashMap<>();

  private final ConcurrentHashMap<JoinFilterColumnCorrelationAnalysis, Set<String>> correlatedValuesCache =
      new ConcurrentHashMap<>();

  public Filter getOrComputeNormalizedFilter(Filter originalFilter)
  {
    Filter filterRef = normalizedFilter;
    if (filterRef == null) {
      synchronized (this) {
        filterRef = normalizedFilter;
        if (filterRef == null) {
          filterRef = Filters.convertToCNF(originalFilter);
          normalizedFilter = filterRef;
        }
      }
    }
    return filterRef;
  }

  public Optional<List<JoinFilterColumnCorrelationAnalysis>> computeCorrelationAnalysisIfAbsent(
      String prefix,
      JoinableClause clause,
      Set<String> baseColumnNames,
      Map<String, Set<Expr>> equiconditions
  )
  {
    Optional<List<JoinFilterColumnCorrelationAnalysis>> correlations = correlationAnalysisCache.computeIfAbsent(
        prefix,
        p -> JoinFilterAnalyzer.findCorrelatedBaseTableColumns(
            baseColumnNames,
            p,
            clause,
            equiconditions
        )
    );

    return correlations;
  }

  public Set<String> computeCorrelatedValuesIfAbsent(
      JoinFilterColumnCorrelationAnalysis correlationAnalysis,
      String selectorFilterDimension,
      String selectorFilterValue,
      String joinColumn,
      JoinableClause joinableClause
  )
  {
    Set<String> correlatedValues = correlatedValuesCache.computeIfAbsent(
        correlationAnalysis,
        ca -> JoinFilterAnalyzer.getCorrelatedValuesForPushDown(
            selectorFilterDimension,
            selectorFilterValue,
            joinColumn,
            joinableClause
        )
    );

    return correlatedValues;
  }



}
