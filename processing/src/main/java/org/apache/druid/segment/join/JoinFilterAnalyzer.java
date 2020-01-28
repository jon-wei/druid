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

package org.apache.druid.segment.join;

import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JoinFilterAnalyzer
{
  public static class JoinFilterSplit
  {
    Filter baseTableFilter;
    Filter joinTableFilter;

    public JoinFilterSplit(
        Filter baseTableFilter,
        Filter joinTableFilter
    )
    {
      this.baseTableFilter = baseTableFilter;
      this.joinTableFilter = joinTableFilter;
    }

    public Filter getBaseTableFilter()
    {
      return baseTableFilter;
    }

    public Filter getJoinTableFilter()
    {
      return joinTableFilter;
    }
  }

  public static class JoinFilterAnalysis
  {
    private final boolean canPushDown;
    private final boolean retainRhs;
    private final Filter originalRhs;
    private final Filter pushdownLhs;

    public JoinFilterAnalysis(
        boolean canPushDown,
        boolean retainRhs,
        Filter originalRhs,
        Filter pushdownLhs
    )
    {
      this.canPushDown = canPushDown;
      this.retainRhs = retainRhs;
      this.originalRhs = originalRhs;
      this.pushdownLhs = pushdownLhs;
    }

    public boolean isCanPushDown()
    {
      return canPushDown;
    }

    public boolean isRetainRhs()
    {
      return retainRhs;
    }

    public Filter getOriginalRhs()
    {
      return originalRhs;
    }

    public Filter getPushdownLhs()
    {
      return pushdownLhs;
    }
  }

  public static JoinFilterSplit splitFilter(
      Filter originalFilter,
      StorageAdapter baseAdapter,
      List<JoinableClause> clauses
  )
  {
    Filter normalizedFilter = Filters.convertToCNF(originalFilter);

    // build the prefix and equicondition maps
    Map<String, JoinableClause> equiconditions = new HashMap<>();
    Map<String, JoinableClause> prefixes = new HashMap<>();
    for (JoinableClause clause : clauses) {
      prefixes.put(clause.getPrefix(), clause);
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        equiconditions.put(clause.getPrefix() + equality.getRightColumn(), clause);
      }
    }

    // List of candidates for pushdown
    // CNF normalization will generate either
    // - an AND filter with multiple subfilters
    // - or a single non-AND subfilter which cannot be split further
    List<Filter> normalizedOrClauses;
    if (normalizedFilter instanceof AndFilter) {
      normalizedOrClauses = ((AndFilter) normalizedFilter).getFilters();
    } else {
      normalizedOrClauses = new ArrayList<>();
      normalizedOrClauses.add(normalizedFilter);
    }

    // Pushdown filters, rewriting if necessary
    List<Filter> leftFilters = new ArrayList<>();
    List<Filter> rightFilters = new ArrayList<>();
    for (Filter orClause : normalizedOrClauses) {
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(orClause, prefixes, equiconditions);
      if (joinFilterAnalysis.isCanPushDown()) {
        leftFilters.add(joinFilterAnalysis.getPushdownLhs());
      }
      if (joinFilterAnalysis.isRetainRhs()) {
        rightFilters.add(joinFilterAnalysis.getOriginalRhs());
      }
    }

    return new JoinFilterSplit(
        leftFilters.isEmpty() ? null : new AndFilter(leftFilters),
        rightFilters.isEmpty() ? null : new AndFilter(rightFilters)
    );
  }

  public static JoinFilterAnalysis analyzeJoinFilterClause(
      Filter filterClause,
      Map<String, JoinableClause> prefixes,
      Map<String, JoinableClause> equiconditions
  )
  {
    // we only support selector filter push down right now
    // IS NULL conditions are not currently supported
    if (filterClause instanceof OrFilter) {
      for (Filter subor : ((OrFilter) filterClause).getFilters()) {
        if (!(subor instanceof SelectorFilter)) {
          return new JoinFilterAnalysis(
              false,
              true,
              filterClause,
              null
          );
        }
      }
      return new JoinFilterAnalysis(
          true,
          false,
          null,
          filterClause
      );
    }

    if (filterClause instanceof SelectorFilter) {
      return new JoinFilterAnalysis(
          true,
          false,
          null,
          rewriteFilterIfRHS(filterClause, prefixes, equiconditions)
      );
    } else {
      return new JoinFilterAnalysis(
          false,
          true,
          filterClause,
          null
      );
    }
  }

  public static Filter rewriteFilterIfRHS(
      Filter filter,
      Map<String, JoinableClause> prefixes,
      Map<String, JoinableClause> equiconditions
  )
  {
    assert(filter instanceof SelectorFilter);
    SelectorFilter selectorFilter = (SelectorFilter) filter;

    String dimName = selectorFilter.getDimension();
    for (String prefix : prefixes.keySet()) {
      if (dimName.startsWith(prefix)) {
        // this filter clause applies to RHS
        // find the rhs table column that's joined to the base table, rewrite into a filter on the base table
       String correlatedBaseTableColumn = findCorrelatedBaseTableColumn(
           prefix,
           dimName,
           prefixes,
           equiconditions
       );
      }
    }
    return filter;
  }

  public static String findCorrelatedBaseTableColumn(
      String prefix,
      String rhsColumnName,
      Map<String, JoinableClause> prefixes,
      Map<String, JoinableClause> equiconditions
  )
  {
    JoinableClause clause = prefixes.get(prefix);
    while (clause != null) {
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        // Is the left hand from another join clause? Try to reach the base segment
        String rhsColumn = equality.getRightColumn();
        String lhsColumn = equality.getLeftExpr().getBindingIfIdentifier();

        // is the lhsColumn a simple identifier? If so, see if we can go further
        // if not, the expr must be on the base table only or we do not push the filter down
        //Expr.BindingDetails bindingDetails = equality.getLeftExpr().analyzeInputs();
        if (lhsColumn == null) {
          // not a simple identifier
          return null;
        } else {
          // is a simple identifier

          clause = equiconditions.get(lhsColumn);
          if (clause == null) {
            // see if it's base table

          }
        }
      }
    }
    return null;
  }
}
