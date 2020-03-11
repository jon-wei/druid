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

import com.google.common.collect.ImmutableList;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.HashJoinSegmentStorageAdapter;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinableClause;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * When there is a filter in a join query, we can sometimes improve performance by applying parts of the filter
 * when we first read from the base table instead of after the join.
 *
 * This class provides a {@link #splitFilter(HashJoinSegmentStorageAdapter, Set, Filter, boolean, boolean)} method that
 * takes a filter and splits it into a portion that should be applied to the base table prior to the join, and a
 * portion that should be applied after the join.
 *
 * The first step of the filter splitting is to convert the filter into
 * https://en.wikipedia.org/wiki/Conjunctive_normal_form (an AND of ORs). This allows us to consider each
 * OR clause independently as a candidate for filter push down to the base table.
 *
 * A filter clause can be pushed down if it meets one of the following conditions:
 * - The filter only applies to columns from the base table
 * - The filter applies to columns from the join table, and we determine that the filter can be rewritten
 *   into a filter on columns from the base table
 *
 * For the second case, where we rewrite filter clauses, the rewritten clause can be less selective than the original,
 * so we preserve the original clause in the post-join filtering phase.
 */
public class JoinFilterAnalyzer
{
  private static final String PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE = "JOIN-FILTER-PUSHDOWN-VIRTUAL-COLUMN-";
  private static final ColumnSelectorFactory ALL_NULL_COLUMN_SELECTOR_FACTORY = new AllNullColumnSelectorFactory();

  public static JoinableClause isColumnFromJoin(
      List<JoinableClause> joinableClauses,
      String column
  )
  {
    for (JoinableClause joinableClause : joinableClauses) {
      if (joinableClause.includesColumn(column)) {
        return joinableClause;
      }
    }

    return null;
  }

  public static boolean isColumnFromPostJoinVirtualColumns(
      List<VirtualColumn> postJoinVirtualColumns,
      String column
  )
  {
    for (VirtualColumn postJoinVirtualColumn : postJoinVirtualColumns) {
      if (column.equals(postJoinVirtualColumn.getOutputName())) {
        return true;
      }
    }
    return false;
  }

  public static boolean areSomeColumnsFromJoin(
      List<JoinableClause> joinableClauses,
      Collection<String> columns
  )
  {
    for (String column : columns) {
      if (isColumnFromJoin(joinableClauses, column) != null) {
        return true;
      }
    }
    return false;
  }

  public static void splitVirtualColumns(
      List<JoinableClause> joinableClauses,
      final VirtualColumns virtualColumns,
      final List<VirtualColumn> preJoinVirtualColumns,
      final List<VirtualColumn> postJoinVirtualColumns
  )
  {
    for (VirtualColumn virtualColumn : virtualColumns.getVirtualColumns()) {
      if(areSomeColumnsFromJoin(joinableClauses, virtualColumn.requiredColumns())) {
        postJoinVirtualColumns.add(virtualColumn);
      } else {
        preJoinVirtualColumns.add(virtualColumn);
      }
    }
  }

  public static JoinFilterPreAnalysis preSplitComputeStuff(
      List<JoinableClause> joinableClauses,
      VirtualColumns virtualColumns,
      Filter originalFilter,
      boolean enableFilterPushDown,
      boolean enableFilterRewrite,
      boolean enableRewriteValueColumnFilters,
      long filterRewriteMaxSize
  )
  {
    final List<VirtualColumn> preJoinVirtualColumns = new ArrayList<>();
    final List<VirtualColumn> postJoinVirtualColumns = new ArrayList<>();

    splitVirtualColumns(joinableClauses, virtualColumns, preJoinVirtualColumns, postJoinVirtualColumns);

    if (originalFilter == null || !enableFilterPushDown) {
      return new JoinFilterPreAnalysis(
          originalFilter,
          null,
          null,
          null,
          enableFilterPushDown,
          enableFilterRewrite
      );
    }

    Filter normalizedFilter = Filters.convertToCNF(originalFilter);

    // List of candidates for pushdown
    // CNF normalization will generate either
    // - an AND filter with multiple subfilters
    // - or a single non-AND subfilter which cannot be split further
    List<Filter> normalizedOrClauses;
    if (normalizedFilter instanceof AndFilter) {
      normalizedOrClauses = ((AndFilter) normalizedFilter).getFilters();
    } else {
      normalizedOrClauses = Collections.singletonList(normalizedFilter);
    }

    List<Filter> normalizedBaseTableClauses = new ArrayList<>();
    List<Filter> normalizedJoinTableClauses = new ArrayList<>();

    for (Filter orClause : normalizedOrClauses) {
      Set<String> reqColumns = orClause.getRequiredColumns();
      if (areSomeColumnsFromJoin(joinableClauses, reqColumns)) {
        normalizedJoinTableClauses.add(orClause);
      } else {
        normalizedBaseTableClauses.add(orClause);
      }
    }

    if (!enableFilterRewrite) {
      return new JoinFilterPreAnalysis(
          originalFilter,
          normalizedBaseTableClauses,
          normalizedJoinTableClauses,
          null,
          enableFilterPushDown,
          enableFilterRewrite
      );
    }

    // build the prefix and equicondition maps
    Map<String, Set<Expr>> equiconditions = new HashMap<>();
    //Map<String, JoinableClause> prefixes = new HashMap<>();
    for (JoinableClause clause : joinableClauses) {
      //prefixes.put(clause.getPrefix(), clause);
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        Set<Expr> exprsForRhs = equiconditions.computeIfAbsent(
            clause.getPrefix() + equality.getRightColumn(),
            (rhs) -> {
              return new HashSet<>();
            }
        );
        exprsForRhs.add(equality.getLeftExpr());
      }
    }

    List<VirtualColumn> pushDownVirtualColumns = new ArrayList<>();
    Map<String, Optional<Map<String, JoinFilterColumnCorrelationAnalysis>>> correlationsByPrefix = new HashMap<>();

    Set<RHSRewriteCandidate> rhsRewriteCandidates = new HashSet<>();
    for (Filter orClause : normalizedJoinTableClauses) {
      if (orClause instanceof SelectorFilter) {
        // this is a candidate for RHS filter rewrite, determine column correlations and correlated values
        String reqColumn = ((SelectorFilter) orClause).getDimension();
        String reqValue = ((SelectorFilter) orClause).getValue();
        JoinableClause joinableClause = isColumnFromJoin(joinableClauses, reqColumn);
        if (joinableClause != null) {
          rhsRewriteCandidates.add(
              new RHSRewriteCandidate(
                  reqColumn,
                  joinableClause,
                  reqValue
              )
          );
        }
      }

      if (orClause instanceof OrFilter) {
        for (Filter subFilter : ((OrFilter) orClause).getFilters()) {
          if (subFilter instanceof SelectorFilter) {
            String reqColumn = ((SelectorFilter) subFilter).getDimension();
            String reqValue = ((SelectorFilter) subFilter).getValue();
            JoinableClause joinableClause = isColumnFromJoin(joinableClauses, reqColumn);
            if (joinableClause != null) {
              rhsRewriteCandidates.add(
                  new RHSRewriteCandidate(
                      reqColumn,
                      joinableClause,
                      reqValue
                  )
              );
            }
          }
        }
      }
    }

    // determine column correlations
    // first build base correlation map for each prefix
    // then attach columnName->filteringValue->correlatedValues
    for (RHSRewriteCandidate rhsRewriteCandidate : rhsRewriteCandidates) {
      Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> correlationsForPrefix = correlationsByPrefix.computeIfAbsent(
          rhsRewriteCandidate.getJoinableClause().getPrefix(),
          p -> findCorrelatedBaseTableColumns2(
              joinableClauses,
              p,
              rhsRewriteCandidate.getJoinableClause(),
              equiconditions
          )
      );
    }

    Map<String, Optional<JoinFilterColumnCorrelationAnalysis>> correlationsByColumn = new HashMap<>();
    for (RHSRewriteCandidate rhsRewriteCandidate : rhsRewriteCandidates) {
      Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> correlationsForPrefix = correlationsByPrefix.get(
          rhsRewriteCandidate.getJoinableClause().getPrefix()
      );
      if (correlationsForPrefix.isPresent()) {
        for (Map.Entry<String, JoinFilterColumnCorrelationAnalysis> correlationForColumn : correlationsForPrefix.get().entrySet()) {
          correlationsByColumn.put(rhsRewriteCandidate.getRhsColumn(), Optional.of(correlationForColumn.getValue()));
          correlationForColumn.getValue().getCorrelatedValuesMap().computeIfAbsent(
              rhsRewriteCandidate.getValueForRewrite(),
              (rhsVal) -> {
                Set<String> correlatedValues = getCorrelatedValuesForPushDown(
                    rhsRewriteCandidate.getRhsColumn(),
                    rhsRewriteCandidate.getValueForRewrite(),
                    correlationForColumn.getValue().getJoinColumn(),
                    rhsRewriteCandidate.getJoinableClause(),
                    enableRewriteValueColumnFilters,
                    filterRewriteMaxSize
                );
                if (correlatedValues.isEmpty()) {
                  return Optional.empty();
                } else {
                  return Optional.of(correlatedValues);
                }
              }
          );
        }
      } else {
        correlationsByColumn.put(rhsRewriteCandidate.getRhsColumn(), Optional.empty());
      }
    }

    return new JoinFilterPreAnalysis(
        originalFilter,
        normalizedBaseTableClauses,
        normalizedJoinTableClauses,
        correlationsByColumn,
        enableFilterPushDown,
        enableFilterRewrite
    );
  }

  private static class RHSRewriteCandidate
  {
    private final String rhsColumn;
    private final JoinableClause joinableClause;
    private final String valueForRewrite;

    public RHSRewriteCandidate(
        String rhsColumn,
        JoinableClause joinableClause,
        String valueForRewrite
    )
    {
      this.rhsColumn = rhsColumn;
      this.joinableClause = joinableClause;
      this.valueForRewrite = valueForRewrite;
    }

    public String getRhsColumn()
    {
      return rhsColumn;
    }

    public JoinableClause getJoinableClause()
    {
      return joinableClause;
    }

    public String getValueForRewrite()
    {
      return valueForRewrite;
    }
  }


  public static JoinFilterSplit splitFilter2(
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    if (joinFilterPreAnalysis.getOriginalFilter() == null || !joinFilterPreAnalysis.isEnableFilterPushDown()) {
      return new JoinFilterSplit(
          null,
          joinFilterPreAnalysis.getOriginalFilter(),
          ImmutableList.of()
      );
    }

    // Pushdown filters, rewriting if necessary
    List<Filter> leftFilters = new ArrayList<>();
    List<Filter> rightFilters = new ArrayList<>();
    List<VirtualColumn> pushDownVirtualColumns = new ArrayList<>();

    for (Filter baseTableFilter : joinFilterPreAnalysis.getNormalizedBaseTableClauses()) {
      if (!filterMatchesNull(baseTableFilter)) {
        leftFilters.add(baseTableFilter);
      } else {
        rightFilters.add(baseTableFilter);
      }
    }

    // Don't need to analyze LHS only filters, push them down always
    //leftFilters.addAll(joinFilterPreAnalysis.getNormalizedBaseTableClauses());

    for (Filter orClause : joinFilterPreAnalysis.getNormalizedJoinTableClauses()) {
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause2(
          orClause,
          joinFilterPreAnalysis
      );
      if (joinFilterAnalysis.isCanPushDown()) {
        leftFilters.add(joinFilterAnalysis.getPushDownFilter().get());
        if (!joinFilterAnalysis.getPushDownVirtualColumns().isEmpty()) {
          pushDownVirtualColumns.addAll(joinFilterAnalysis.getPushDownVirtualColumns());
        }
      }
      if (joinFilterAnalysis.isRetainAfterJoin()) {
        rightFilters.add(joinFilterAnalysis.getOriginalFilter());
      }
    }

    return new JoinFilterSplit(
        Filters.and(leftFilters),
        Filters.and(rightFilters),
        pushDownVirtualColumns
    );
  }


  private static JoinFilterAnalysis analyzeJoinFilterClause2(
      Filter filterClause,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    // NULL matching conditions are not currently pushed down.
    // They require special consideration based on the join type, and for simplicity of the initial implementation
    // this is not currently handled.
    if (!joinFilterPreAnalysis.isEnableFilterRewrite() || filterMatchesNull(filterClause)) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
    }

    // Currently we only support rewrites of selector filters and selector filters within OR filters.
    if (filterClause instanceof SelectorFilter) {
      return rewriteSelectorFilter2(
          (SelectorFilter) filterClause,
          joinFilterPreAnalysis
      );
    }

    if (filterClause instanceof OrFilter) {
      return rewriteOrFilter2(
          (OrFilter) filterClause,
          joinFilterPreAnalysis
      );
    }

    return JoinFilterAnalysis.createNoPushdownFilterAnalysis(filterClause);
  }

  private static JoinFilterAnalysis rewriteOrFilter2(
      OrFilter orFilter,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    boolean retainRhs = false;
    List<Filter> newFilters = new ArrayList<>();
    for (Filter filter : orFilter.getFilters()) {
      retainRhs = true;
      if (filter instanceof SelectorFilter) {
        JoinFilterAnalysis rewritten = rewriteSelectorFilter2(
            (SelectorFilter) filter,
            joinFilterPreAnalysis
        );
        if (!rewritten.isCanPushDown()) {
          return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
        } else {
          newFilters.add(rewritten.getPushDownFilter().get());
        }
      } else {
        return JoinFilterAnalysis.createNoPushdownFilterAnalysis(orFilter);
      }
    }

    return new JoinFilterAnalysis(
        retainRhs,
        orFilter,
        new OrFilter(newFilters),
        ImmutableList.of()
    );
  }

  private static JoinFilterAnalysis rewriteSelectorFilter2(
      SelectorFilter selectorFilter,
      JoinFilterPreAnalysis joinFilterPreAnalysis
  )
  {
    String filteringColumn = selectorFilter.getDimension();
    String filteringValue = selectorFilter.getValue();

    Optional<JoinFilterColumnCorrelationAnalysis> correlationAnalysis = joinFilterPreAnalysis.getCorrelationsByColumn().get(filteringColumn);

    if (!correlationAnalysis.isPresent()) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
    }

    List<Filter> newFilters = new ArrayList<>();
    List<VirtualColumn> pushdownVirtualColumns = new ArrayList<>();

    if (correlationAnalysis.get().supportsPushDown()) {
      Optional<Set<String>> correlatedValues = correlationAnalysis.get().getCorrelatedValuesMap().get(filteringValue);

      if (!correlatedValues.isPresent()) {
        return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
      }

      for (String correlatedBaseColumn : correlationAnalysis.get().getBaseColumns()) {
        Filter rewrittenFilter = new InDimFilter(
            correlatedBaseColumn,
            correlatedValues.get(),
            null,
            null
        ).toFilter();
        newFilters.add(rewrittenFilter);
      }

      for (Expr correlatedBaseExpr : correlationAnalysis.get().getBaseExpressions()) {
        // We need to create a virtual column for the expressions when pushing down.
        // Note that this block is never entered right now, since correlationAnalysis.supportsPushDown()
        // will return false if there any correlated expressions on the base table.
        // Pushdown of such filters is disabled until the expressions system supports converting an expression
        // into a String representation that can be reparsed into the same expression.
        // https://github.com/apache/druid/issues/9326 tracks this expressions issue.
        String vcName = getCorrelatedBaseExprVirtualColumnName(pushdownVirtualColumns.size());

        VirtualColumn correlatedBaseExprVirtualColumn = new ExpressionVirtualColumn(
            vcName,
            correlatedBaseExpr,
            ValueType.STRING
        );
        pushdownVirtualColumns.add(correlatedBaseExprVirtualColumn);

        Filter rewrittenFilter = new InDimFilter(
            vcName,
            correlatedValues.get(),
            null,
            null
        ).toFilter();
        newFilters.add(rewrittenFilter);
      }
    }

    if (newFilters.isEmpty()) {
      return JoinFilterAnalysis.createNoPushdownFilterAnalysis(selectorFilter);
    }

    return new JoinFilterAnalysis(
        true,
        selectorFilter,
        Filters.and(newFilters),
        pushdownVirtualColumns
    );
  }

  private static String getCorrelatedBaseExprVirtualColumnName(int counter)
  {
    // May want to have this check other column names to absolutely prevent name conflicts
    return PUSH_DOWN_VIRTUAL_COLUMN_NAME_BASE + counter;
  }

  /**
   * Helper method for rewriting filters on join table columns into filters on base table columns.
   *
   * @param filterColumn           A join table column that we're filtering on
   * @param filterValue            The value to filter on
   * @param correlatedJoinColumn   A join table column that appears as the RHS of an equicondition, which we can correlate
   *                               with a column on the base table
   * @param clauseForFilteredTable The joinable clause that corresponds to the join table being filtered on
   *
   * @return A list of values of the correlatedJoinColumn that appear in rows where filterColumn = filterValue
   * Returns an empty set if we cannot determine the correlated values.
   */
  private static Set<String> getCorrelatedValuesForPushDown(
      String filterColumn,
      String filterValue,
      String correlatedJoinColumn,
      JoinableClause clauseForFilteredTable,
      boolean enableRewriteValueColumnFilters,
      long filterRewriteMaxSize
  )
  {
    String filterColumnNoPrefix = filterColumn.substring(clauseForFilteredTable.getPrefix().length());
    String correlatedColumnNoPrefix = correlatedJoinColumn.substring(clauseForFilteredTable.getPrefix().length());

    return clauseForFilteredTable.getJoinable().getCorrelatedColumnValues(
        filterColumnNoPrefix,
        filterValue,
        correlatedColumnNoPrefix,
        filterRewriteMaxSize,
        enableRewriteValueColumnFilters
    );
  }

  private static Optional<Map<String, JoinFilterColumnCorrelationAnalysis>> findCorrelatedBaseTableColumns2(
      List<JoinableClause> joinableClauses,
      String tablePrefix,
      JoinableClause clauseForTablePrefix,
      Map<String, Set<Expr>> equiConditions
  )
  {
    JoinConditionAnalysis jca = clauseForTablePrefix.getCondition();

    Set<String> rhsColumns = new HashSet<>();
    for (Equality eq : jca.getEquiConditions()) {
      rhsColumns.add(tablePrefix + eq.getRightColumn());
    }

    Map<String, JoinFilterColumnCorrelationAnalysis> correlations = new HashMap<>();

    for (String rhsColumn : rhsColumns) {
      Set<String> correlatedBaseColumns = new HashSet<>();
      Set<Expr> correlatedBaseExpressions = new HashSet<>();

      getCorrelationForRHSColumn2(
          joinableClauses,
          equiConditions,
          rhsColumn,
          correlatedBaseColumns,
          correlatedBaseExpressions
      );

      if (correlatedBaseColumns.isEmpty() && correlatedBaseExpressions.isEmpty()) {
        continue;
      }

      correlations.put(
          rhsColumn,
          new JoinFilterColumnCorrelationAnalysis(
              rhsColumn,
              correlatedBaseColumns,
              correlatedBaseExpressions
          )
      );
    }

    //List<JoinFilterColumnCorrelationAnalysis> dedupCorrelations = eliminateCorrelationDuplicates(correlations);

    if (correlations.size() == 0) {
      return Optional.empty();
    } else {
      return Optional.of(correlations);
    }
  }

  private static void getCorrelationForRHSColumn2(
      List<JoinableClause> joinableClauses,
      Map<String, Set<Expr>> equiConditions,
      String rhsColumn,
      Set<String> correlatedBaseColumns,
      Set<Expr> correlatedBaseExpressions
  )
  {
    String findMappingFor = rhsColumn;
    Set<Expr> lhsExprs = equiConditions.get(findMappingFor);
    if (lhsExprs == null) {
      return;
    }

    for (Expr lhsExpr : lhsExprs) {
      String identifier = lhsExpr.getBindingIfIdentifier();
      if (identifier == null) {
        // We push down if the function only requires base table columns
        Expr.BindingDetails bindingDetails = lhsExpr.analyzeInputs();
        Set<String> requiredBindings = bindingDetails.getRequiredBindings();

        if (areSomeColumnsFromJoin(joinableClauses, requiredBindings)) {
          break;
        }
        correlatedBaseExpressions.add(lhsExpr);
      } else {
        // simple identifier, see if we can correlate it with a column on the base table
        findMappingFor = identifier;
        if (isColumnFromJoin(joinableClauses, identifier) == null) {
          correlatedBaseColumns.add(findMappingFor);
        } else {
          getCorrelationForRHSColumn2(
              joinableClauses,
              equiConditions,
              findMappingFor,
              correlatedBaseColumns,
              correlatedBaseExpressions
          );
        }
      }
    }
  }

  /**
   * Given a list of JoinFilterColumnCorrelationAnalysis, prune the list so that we only have one
   * JoinFilterColumnCorrelationAnalysis for each unique combination of base columns.
   *
   * Suppose we have a join condition like the following, where A is the base table:
   *   A.joinColumn == B.joinColumn && A.joinColumn == B.joinColumn2
   *
   * We only need to consider one correlation to A.joinColumn since B.joinColumn and B.joinColumn2 must
   * have the same value in any row that matches the join condition.
   *
   * In the future this method could consider which column correlation should be preserved based on availability of
   * indices and other heuristics.
   *
   * When push down of filters with LHS expressions in the join condition is supported, this method should also
   * consider expressions.
   *
   * @param originalList Original list of column correlation analyses.
   * @return Pruned list of column correlation analyses.
   */
  private static List<JoinFilterColumnCorrelationAnalysis> eliminateCorrelationDuplicates(
      List<JoinFilterColumnCorrelationAnalysis> originalList
  )
  {
    Map<List<String>, JoinFilterColumnCorrelationAnalysis> uniquesMap = new HashMap<>();
    for (JoinFilterColumnCorrelationAnalysis jca : originalList) {
      uniquesMap.put(jca.getBaseColumns(), jca);
    }

    return new ArrayList<>(uniquesMap.values());
  }

  private static boolean filterMatchesNull(Filter filter)
  {
    ValueMatcher valueMatcher = filter.makeMatcher(ALL_NULL_COLUMN_SELECTOR_FACTORY);
    return valueMatcher.matches();
  }
}
