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

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.InFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.druid.segment.join.lookup.LookupJoinable;
import org.apache.druid.segment.join.table.IndexedTable;
import org.apache.druid.segment.join.table.IndexedTableJoinable;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JoinFilterAnalyzer
{
  public static class JoinFilterSplit
  {
    final Filter baseTableFilter;
    final Filter joinTableFilter;
    final List<VirtualColumn> pushdownLhsVirtualColumns;

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

    public List<VirtualColumn> getPushdownLhsVirtualColumns()
    {
      return pushdownLhsVirtualColumns;
    }
  }

  public static class JoinFilterAnalysis
  {
    private final boolean canPushDown;
    private final boolean retainRhs;
    private final Filter originalRhs;
    private final Filter pushdownLhs;

    private final List<VirtualColumn> pushdownLhsVirtualColumns;

    public JoinFilterAnalysis(
        boolean canPushDown,
        boolean retainRhs,
        Filter originalRhs,
        Filter pushdownLhs,
        List<VirtualColumn> pushdownLhsVirtualColumns
    )
    {
      this.canPushDown = canPushDown;
      this.retainRhs = retainRhs;
      this.originalRhs = originalRhs;
      this.pushdownLhs = pushdownLhs;
      this.pushdownLhsVirtualColumns = pushdownLhsVirtualColumns;
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

    public List<VirtualColumn> getPushdownLhsVirtualColumns()
    {
      return pushdownLhsVirtualColumns;
    }
  }

  public static class JoinFilterColumnCorrelationAnalysis
  {
    private String rhsColumn;
    private List<String> baseColumns;
    private List<Expr> baseExpressions;

    public JoinFilterColumnCorrelationAnalysis(
        String rhsColumn,
        List<String> baseColumns,
        List<Expr> baseExpressions
    )
    {
      this.rhsColumn = rhsColumn;
      this.baseColumns = baseColumns;
      this.baseExpressions = baseExpressions;
    }

    public String getRhsColumn()
    {
      return rhsColumn;
    }

    public List<String> getBaseColumns()
    {
      return baseColumns;
    }

    public List<Expr> getBaseExpressions()
    {
      return baseExpressions;
    }

    public boolean supportsPushDown()
    {
      return !baseColumns.isEmpty() || !baseExpressions.isEmpty();
    }
  }

  public static JoinFilterSplit splitFilter(
      Filter originalFilter,
      HashJoinSegmentStorageAdapter baseAdapter,
      List<JoinableClause> clauses
  )
  {
    Filter normalizedFilter = Filters.convertToCNF(originalFilter);

    // build the prefix and equicondition maps
    Map<String, Expr> equiconditions = new HashMap<>();
    Map<String, JoinableClause> prefixes = new HashMap<>();
    for (JoinableClause clause : clauses) {
      prefixes.put(clause.getPrefix(), clause);
      for (Equality equality : clause.getCondition().getEquiConditions()) {
        equiconditions.put(clause.getPrefix() + equality.getRightColumn(), equality.getLeftExpr());
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
      JoinFilterAnalysis joinFilterAnalysis = analyzeJoinFilterClause(baseAdapter, orClause, prefixes, equiconditions);
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
      HashJoinSegmentStorageAdapter baseAdapter,
      Filter filterClause,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions
  )
  {
    // we only support selector filter push down right now
    // IS NULL conditions are not currently supported
    if (filterClause instanceof OrFilter) {
      //TODO
      return new JoinFilterAnalysis(
          true,
          false,
          null,
          filterClause,
          null
      );
    }

    if (filterClause instanceof SelectorFilter) {
      return rewriteSelectorFilter(
          baseAdapter,
          filterClause,
          prefixes,
          equiconditions
      );
    } else {
      return new JoinFilterAnalysis(
          false,
          true,
          filterClause,
          null,
          null
      );
    }
  }

  @Nullable
  public static JoinFilterAnalysis rewriteOrFilter(
      HashJoinSegmentStorageAdapter baseAdapter,
      OrFilter orFilter,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions
  )
  {
    boolean retainRhs = false;

    List<Filter> newFilters = new ArrayList<>();
    for (Filter filter : orFilter.getFilters()) {
      for (String requiredColumn : filter.getRequiredColumns()) {
        if (!baseAdapter.isBaseColumn(requiredColumn)) {
          if (filter instanceof SelectorFilter) {
            JoinFilterAnalysis rewritten = rewriteSelectorFilter(
                baseAdapter,
                filter,
                prefixes,
                equiconditions
            );
            if (!rewritten.isCanPushDown()) {
              return new JoinFilterAnalysis(
                  false,
                  true,
                  orFilter,
                  null,
                  null
              );
            } else {
              newFilters.add(rewritten.getPushdownLhs());
            }
          } else {
            return new JoinFilterAnalysis(
                false,
                true,
                orFilter,
                null,
                null
            );
          }
        } else {
          newFilters.add(filter);
        }
      }
    }

    return new JoinFilterAnalysis(
        true,
        retainRhs,
        orFilter,
        new OrFilter(newFilters),
        null
    );
  }

  public static JoinFilterAnalysis rewriteSelectorFilter(
      HashJoinSegmentStorageAdapter baseAdapter,
      Filter filter,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions
  )
  {
    assert(filter instanceof SelectorFilter);
    SelectorFilter selectorFilter = (SelectorFilter) filter;

    String dimName = selectorFilter.getDimension();
    for (String prefix : prefixes.keySet()) {
      if (dimName.startsWith(prefix)) {
        // this filter clause applies to RHS
        // analyze the clause for the RHS table that this filter is applied to, find correlated RHS columns
        // and base table columns
        List<JoinFilterColumnCorrelationAnalysis> correlations = findCorrelatedBaseTableColumns(
           baseAdapter,
           prefix,
           prefixes,
           equiconditions
        );

        List<Filter> newFilters = new ArrayList<>();
        List<VirtualColumn> pushdownVirtualColumns = new ArrayList<>();

        for (JoinFilterColumnCorrelationAnalysis correlationAnalysis : correlations) {
          if (correlationAnalysis.supportsPushDown()) {
            List<String> correlatedValues = getCorrelatedValuesForPushDown(
                selectorFilter.getDimension(),
                selectorFilter.getValue(),
                prefixes.get(prefix),
                correlationAnalysis
            );

            for (String correlatedBaseColumn : correlationAnalysis.getBaseColumns()) {
              InFilter rewrittenFilter = (InFilter) new InDimFilter(
                  correlatedBaseColumn,
                  correlatedValues,
                  null,
                  null
              ).toFilter();
              newFilters.add(rewrittenFilter);
            }

            for (Expr correlatedBaseExpr : correlationAnalysis.getBaseExpressions()) {
              // need to create a virtual column for the expressions when pushing down
              String vcName = getCorrelatedBaseExprVirtualColumnName(pushdownVirtualColumns.size());

              VirtualColumn correlatedBaseExprVirtualColumn = new ExpressionVirtualColumn(
                  vcName,
                  correlatedBaseExpr.toString(),
                  ValueType.STRING,
                  ExprMacroTable.nil() // TODO: Need an injected ExprMacroTable
              );
              pushdownVirtualColumns.add(correlatedBaseExprVirtualColumn);

              InFilter rewrittenFilter = (InFilter) new InDimFilter(
                  vcName,
                  correlatedValues,
                  null,
                  null
              ).toFilter();
              newFilters.add(rewrittenFilter);
            }
          }
        }

        return new JoinFilterAnalysis(
            true,
            true,
            filter,
            newFilters.size() == 1 ? newFilters.get(0) : new AndFilter(newFilters),
            null
        );
      }
    }
    return new JoinFilterAnalysis(
        true,
        false,
        filter,
        filter,
        null
    );
  }

  public static String getCorrelatedBaseExprVirtualColumnName(int counter)
  {
    return "TEST-TEST-VIRTUAL-COLUMN-" + counter;
  }

  public static List<String> getCorrelatedValuesForPushDown(
      String dimName,
      String filterValue,
      JoinableClause clause,
      JoinFilterColumnCorrelationAnalysis correlationAnalysis
  )
  {
    // would be nice to have non-key column indices on the Joinables for better perf
    if (clause.getJoinable() instanceof LookupJoinable) {
      LookupJoinable lookupJoinable = (LookupJoinable) clause.getJoinable();
      List<String> correlatedValues = lookupJoinable.getExtractor().unapply(filterValue);
      return correlatedValues;
    }

    String dimNameNoPrefix = dimName.substring(clause.getPrefix().length());
    String rhsColumnNoPrefix = correlationAnalysis.getRhsColumn().substring(clause.getPrefix().length());

    if (clause.getJoinable() instanceof IndexedTableJoinable) {
      IndexedTableJoinable indexedTableJoinable = (IndexedTableJoinable) clause.getJoinable();
      IndexedTable indexedTable = indexedTableJoinable.getTable();

      int dimNameColumnPosition = indexedTable.allColumns().indexOf(dimNameNoPrefix);
      int correlatedColumnPosition = indexedTable.allColumns().indexOf(rhsColumnNoPrefix);

      if (indexedTable.keyColumns().contains(dimNameNoPrefix)) {
        IndexedTable.Index index = indexedTable.columnIndex(dimNameColumnPosition);
        IndexedTable.Reader reader = indexedTable.columnReader(correlatedColumnPosition);
        IntList rowIndex = index.find(filterValue);
        List<String> correlatedValues = new ArrayList<>();
        for (int i = 0; i < rowIndex.size(); i++) {
          int rowNum = rowIndex.getInt(i);
          correlatedValues.add(reader.read(rowNum).toString());
        }
        return correlatedValues;
      } else {
        IndexedTable.Reader dimNameReader = indexedTable.columnReader(dimNameColumnPosition);
        IndexedTable.Reader correlatedColumnReader = indexedTable.columnReader(correlatedColumnPosition);
        Set<String> correlatedValueSet = new HashSet<>();
        for (int i = 0; i < indexedTable.numRows(); i++) {
          if (filterValue.equals(dimNameReader.read(i).toString())) {
            correlatedValueSet.add(correlatedColumnReader.read(i).toString());
          }
        }

        return new ArrayList<>(correlatedValueSet);
      }
    }

    return null;
  }

  public static List<JoinFilterColumnCorrelationAnalysis> findCorrelatedBaseTableColumns(
      HashJoinSegmentStorageAdapter baseAdapter,
      String prefix,
      Map<String, JoinableClause> prefixes,
      Map<String, Expr> equiconditions
  )
  {
    JoinableClause clause = prefixes.get(prefix);
    JoinConditionAnalysis jca =  clause.getCondition();

    List<String> rhsColumns = new ArrayList<>();
    for (Equality eq : jca.getEquiConditions()) {
      rhsColumns.add(prefix + eq.getRightColumn());
    }

    List<JoinFilterColumnCorrelationAnalysis> correlations = new ArrayList<>();

    for (String rhsColumn : rhsColumns) {
      List<String> correlatedBaseColumns = new ArrayList<>();
      List<Expr> correlatedBaseExpressions = new ArrayList<>();
      boolean terminate = false;

      String findMappingFor = rhsColumn;
      while (!terminate) {
        Expr lhs = equiconditions.get(findMappingFor);
        if (lhs == null) {
          break;
        }
        String identifier = lhs.getBindingIfIdentifier();
        if (identifier == null) {
          // if it's a function on a column of the base table, we can still push down.
          // if it's a function on a non-base table column, skip push down for now.
          Expr.BindingDetails bindingDetails = lhs.analyzeInputs();
          assert (bindingDetails.getFreeVariables().size() == 1);
          Expr baseExpr = bindingDetails.getFreeVariables().iterator().next();
          String baseColumn = baseExpr.getBindingIfIdentifier();

          terminate = true;
          if (baseAdapter.isBaseColumn(baseColumn)) {
            correlatedBaseExpressions.add(lhs);
          }
        } else {
          // simple identifier, see if we can correlate it with a column on the base table
          findMappingFor = identifier;
          if (baseAdapter.isBaseColumn(identifier)) {
            terminate = true;
            correlatedBaseColumns.add(findMappingFor);
          }
        }
      }

      correlations.add(
          new JoinFilterColumnCorrelationAnalysis(
              rhsColumn,
              correlatedBaseColumns,
              correlatedBaseExpressions
          )
      );
    }

    return correlations;
  }
}
