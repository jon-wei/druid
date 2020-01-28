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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Capabilities;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class HashJoinSegmentStorageAdapter implements StorageAdapter
{
  private final StorageAdapter baseAdapter;
  private final List<JoinableClause> clauses;

  HashJoinSegmentStorageAdapter(
      StorageAdapter baseAdapter,
      List<JoinableClause> clauses
  )
  {
    this.baseAdapter = baseAdapter;
    this.clauses = clauses;
  }

  @Override
  public Interval getInterval()
  {
    return baseAdapter.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    // Use a Set since we may encounter duplicates, if a field from a Joinable shadows one of the base fields.
    final LinkedHashSet<String> availableDimensions = new LinkedHashSet<>();

    baseAdapter.getAvailableDimensions().forEach(availableDimensions::add);

    for (JoinableClause clause : clauses) {
      availableDimensions.addAll(clause.getAvailableColumnsPrefixed());
    }

    return new ListIndexed<>(Lists.newArrayList(availableDimensions));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    final Optional<JoinableClause> maybeClause = getClauseForColumn(column);

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      return clause.getJoinable().getCardinality(clause.unprefix(column));
    } else {
      return baseAdapter.getDimensionCardinality(column);
    }
  }

  @Override
  public DateTime getMinTime()
  {
    return baseAdapter.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return baseAdapter.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    if (isBaseColumn(column)) {
      return baseAdapter.getMinValue(column);
    } else {
      return null;
    }
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    if (isBaseColumn(column)) {
      return baseAdapter.getMaxValue(column);
    } else {
      return null;
    }
  }

  @Override
  public Capabilities getCapabilities()
  {
    // Dictionaries in the joinables may not be sorted. Unfortunately this API does not let us be granular about what
    // is and isn't sorted, so return false globally. At the time of this writing, the only query affected by this
    // is a topN with lexicographic sort and 'previousStop' set (it will not be able to skip values based on
    // dictionary code).
    return Capabilities.builder().dimensionValuesSorted(false).build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final Optional<JoinableClause> maybeClause = getClauseForColumn(column);

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      return clause.getJoinable().getColumnCapabilities(clause.unprefix(column));
    } else {
      return baseAdapter.getColumnCapabilities(column);
    }
  }

  @Nullable
  @Override
  public String getColumnTypeName(String column)
  {
    final Optional<JoinableClause> maybeClause = getClauseForColumn(column);

    if (maybeClause.isPresent()) {
      final JoinableClause clause = maybeClause.get();
      final ColumnCapabilities capabilities = clause.getJoinable().getColumnCapabilities(clause.unprefix(column));
      return capabilities != null ? capabilities.getType().toString() : null;
    } else {
      return baseAdapter.getColumnTypeName(column);
    }
  }

  @Override
  public int getNumRows()
  {
    // Cannot determine the number of rows ahead of time for a join segment (rows may be added or removed based
    // on the join condition). At the time of this writing, this method is only used by the 'segmentMetadata' query,
    // which isn't meant to support join segments anyway.
    throw new UnsupportedOperationException("Cannot retrieve number of rows from join segment");
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return baseAdapter.getMaxIngestedEventTime();
  }

  @Override
  public Metadata getMetadata()
  {
    // Cannot get meaningful Metadata for this segment, since it isn't real. At the time of this writing, this method
    // is only used by the 'segmentMetadata' query, which isn't meant to support join segments anyway.
    throw new UnsupportedOperationException("Cannot retrieve metadata from join segment");
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      @Nonnull final Interval interval,
      @Nonnull final VirtualColumns virtualColumns,
      @Nonnull final Granularity gran,
      final boolean descending,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    final Set<String> baseColumns = new HashSet<>();
    Iterables.addAll(baseColumns, baseAdapter.getAvailableDimensions());
    Iterables.addAll(baseColumns, baseAdapter.getAvailableMetrics());

    final List<VirtualColumn> preJoinVirtualColumns = new ArrayList<>();
    final List<VirtualColumn> postJoinVirtualColumns = new ArrayList<>();

    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      // Virtual columns cannot depend on each other, so we don't need to check transitive dependencies.
      if (baseColumns.containsAll(virtualColumn.requiredColumns())) {
        preJoinVirtualColumns.add(virtualColumn);
      } else {
        postJoinVirtualColumns.add(virtualColumn);
      }
    }

    JoinFilterSplit joinFilterSplit = JoinFilterSplit.splitFilter(
        filter,
        baseAdapter,
        clauses
    );

    // Soon, we will need a way to push filters past a join when possible. This could potentially be done right here
    // (by splitting out pushable pieces of 'filter') or it could be done at a higher level (i.e. in the SQL planner).
    //
    // If it's done in the SQL planner, that will likely mean adding a 'baseFilter' parameter to this class that would
    // be passed in to the below baseAdapter.makeCursors call (instead of the null filter).
    final Sequence<Cursor> baseCursorSequence = baseAdapter.makeCursors(
        joinFilterSplit.getBaseTableFilter(),
        interval,
        VirtualColumns.create(preJoinVirtualColumns),
        gran,
        descending,
        queryMetrics
    );

    return Sequences.map(
        baseCursorSequence,
        cursor -> {
          Cursor retVal = cursor;

          for (JoinableClause clause : clauses) {
            retVal = HashJoinEngine.makeJoinCursor(retVal, clause);
          }

          return PostJoinCursor.wrap(
              retVal,
              VirtualColumns.create(postJoinVirtualColumns),
              joinFilterSplit.getJoinTableFilter()
          );
        }
    );
  }

  /**
   * Returns whether "column" will be selected from "baseAdapter". This is true if it is not shadowed by any joinables
   * (i.e. if it does not start with any of their prefixes).
   */
  private boolean isBaseColumn(final String column)
  {
    return !getClauseForColumn(column).isPresent();
  }

  /**
   * Returns the JoinableClause corresponding to a particular column, based on the clauses' prefixes.
   *
   * @param column column name
   *
   * @return the clause, or absent if the column does not correspond to any clause
   */
  private Optional<JoinableClause> getClauseForColumn(final String column)
  {
    // Check clauses in reverse, since "makeCursors" creates the cursor in such a way that the last clause
    // gets first dibs to claim a column.
    return Lists.reverse(clauses)
                .stream()
                .filter(clause -> clause.includesColumn(column))
                .findFirst();
  }

  public static class JoinFilterAnalysis
  {
    private boolean canPushDown;
    private boolean retainRhs;
    private Filter originalRhs;
    private Filter pushdownLhs;

    public static JoinFilterAnalysis CANNOT_PUSH_DOWN = new JoinFilterAnalysis(
        false,
        true,
        null,
        null
    );

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

    public static JoinFilterSplit splitFilter(
        Filter originalFilter,
        StorageAdapter baseAdapter,
        List<JoinableClause> clauses
    )
    {
      Filter normalizedFilter = Filters.convertToCNF(originalFilter);

      // build the equicondition map
      Map<String, JoinableClause> equiconditions = new HashMap<>();
      for (JoinableClause clause : clauses) {
        for (Equality equality : clause.getCondition().getEquiConditions()) {
          equiconditions.put(equality.getRightColumn(), clause);
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
        JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(orClause, equiconditions);
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
  }

  public static JoinFilterAnalysis analyzeJoinFilterClause(
      Filter filterClause,
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
          filterClause
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
}
