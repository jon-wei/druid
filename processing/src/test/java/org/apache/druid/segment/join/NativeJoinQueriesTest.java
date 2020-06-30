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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import junitparams.Parameters;
import org.apache.druid.annotations.UsedByJUnitParamsRunner;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Druids;
import org.apache.druid.query.JoinDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.expression.IPv4AddressMatchExprMacro;
import org.apache.druid.query.expression.IPv4AddressParseExprMacro;
import org.apache.druid.query.expression.IPv4AddressStringifyExprMacro;
import org.apache.druid.query.expression.LikeExprMacro;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.expression.RegexpExtractExprMacro;
import org.apache.druid.query.expression.RegexpLikeExprMacro;
import org.apache.druid.query.expression.TimestampCeilExprMacro;
import org.apache.druid.query.expression.TimestampExtractExprMacro;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.query.expression.TimestampFormatExprMacro;
import org.apache.druid.query.expression.TimestampParseExprMacro;
import org.apache.druid.query.expression.TimestampShiftExprMacro;
import org.apache.druid.query.expression.TrimExprMacro;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NativeJoinQueriesTest
{
  private static final ImmutableMap.Builder<String, Object> DEFAULT_QUERY_CONTEXT_BUILDER =
      ImmutableMap.<String, Object>builder()
          .put(QueryContexts.DEFAULT_TIMEOUT_KEY, QueryContexts.DEFAULT_TIMEOUT_MILLIS)
          .put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, Long.MAX_VALUE);
  public static final Map<String, Object> QUERY_CONTEXT_DEFAULT = DEFAULT_QUERY_CONTEXT_BUILDER.build();

  public static final String DATASOURCE1 = "foo";

  public static QueryRunnerFactoryConglomerate conglomerate;
  public SpecificSegmentsQuerySegmentWalker walker = null;

  @Test
  @Parameters(source = QueryContextForJoinProvider.class)
  public void testGroupByJoinAsNativeQueryWithUnoptimizedFilter(Map<String, Object> queryContext) throws Exception
  {
    // The query below is the same as the inner groupBy on a join datasource from the test
    // testNestedGroupByOnInlineDataSourceWithFilter, except that the selector filter
    // dim1=def has been rewritten into an unoptimized filter, dim1 IN (def).
    //
    // The unoptimized filter will be optimized into dim1=def by the query toolchests in their
    // pre-merge decoration function, when it calls DimFilter.optimize().
    //
    // This test's goal is to ensure that the join filter rewrites function correctly when there are
    // unoptimized filters in the join query. The rewrite logic must apply to the optimized form of the filters,
    // as this is what will be passed to HashJoinSegmentAdapter.makeCursors(), where the result of the join
    // filter pre-analysis is used.
    //
    // A native query is used because the filter types where we support optimization are the AND/OR/NOT and
    // IN filters. However, when expressed in a SQL query, our SQL planning layer is smart enough to already apply
    // these optimizations in the native query it generates, making it impossible to test the unoptimized filter forms
    // using SQL queries.
    Query query = GroupByQuery
        .builder()
        .setDataSource(
            join(
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                new QueryDataSource(
                    newScanQueryBuilder()
                        .dataSource(DATASOURCE1)
                        .intervals(querySegmentSpec(Intervals.of("2001-01-02T00:00:00.000Z/146140482-04-24T15:36:27.903Z")))
                        .columns("dim1", "m2")
                        .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                        .context(queryContext)
                        .build()
                ),
                "j0.",
                equalsCondition("dim1", "j0.dim1"),
                JoinType.INNER
            )
        )
        .setGranularity(Granularities.ALL)
        .setInterval(querySegmentSpec(Intervals.ETERNITY))
        .setDimFilter(in("dim1", Collections.singletonList("def"), null))  // provide an unoptimized IN filter
        .setDimensions(
            dimensions(
                new DefaultDimensionSpec("v0", "d0")
            )
        )
        .setVirtualColumns(expressionVirtualColumn("v0", "'def'", ValueType.STRING))
        .build();

    QueryLifecycleFactory qlf = CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate);
    QueryLifecycle ql = qlf.factorize();
    Sequence seq = ql.runSimple(
        query,
        CalciteTests.SUPER_USER_AUTH_RESULT,
        null
    );
    List<Object> results = seq.toList();
    Assert.assertEquals(
        ImmutableList.of(ResultRow.of("def")),
        results
    );
  }

  public static JoinDataSource join(
      DataSource left,
      DataSource right,
      String rightPrefix,
      String condition,
      JoinType joinType
  )
  {
    return JoinDataSource.create(
        left,
        right,
        rightPrefix,
        condition,
        joinType,
        createExprMacroTable()
    );
  }

  public static Druids.ScanQueryBuilder newScanQueryBuilder()
  {
    return new Druids.ScanQueryBuilder().resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                                        .legacy(false);
  }

  public static QuerySegmentSpec querySegmentSpec(final Interval... intervals)
  {
    return new MultipleIntervalSegmentSpec(Arrays.asList(intervals));
  }

  public static String equalsCondition(String left, String right)
  {
    return StringUtils.format("(%s == %s)", left, right);
  }

  public static List<DimensionSpec> dimensions(final DimensionSpec... dimensionSpecs)
  {
    return Arrays.asList(dimensionSpecs);
  }

  public static InDimFilter in(String dimension, List<String> values, ExtractionFn extractionFn)
  {
    return new InDimFilter(dimension, values, extractionFn);
  }

  public static ExpressionVirtualColumn expressionVirtualColumn(
      final String name,
      final String expression,
      final ValueType outputType
  )
  {
    return new ExpressionVirtualColumn(name, expression, outputType, createExprMacroTable());
  }

  public static final List<Class<? extends ExprMacroTable.ExprMacro>> EXPR_MACROS =
      ImmutableList.<Class<? extends ExprMacroTable.ExprMacro>>builder()
          .add(IPv4AddressMatchExprMacro.class)
          .add(IPv4AddressParseExprMacro.class)
          .add(IPv4AddressStringifyExprMacro.class)
          .add(LikeExprMacro.class)
          .add(RegexpExtractExprMacro.class)
          .add(RegexpLikeExprMacro.class)
          .add(TimestampCeilExprMacro.class)
          .add(TimestampExtractExprMacro.class)
          .add(TimestampFloorExprMacro.class)
          .add(TimestampFormatExprMacro.class)
          .add(TimestampParseExprMacro.class)
          .add(TimestampShiftExprMacro.class)
          .add(TrimExprMacro.BothTrimExprMacro.class)
          .add(TrimExprMacro.LeftTrimExprMacro.class)
          .add(TrimExprMacro.RightTrimExprMacro.class)
          .build();

  public static final Injector INJECTOR = Guice.createInjector(
      binder -> {
        binder.bind(Key.get(ObjectMapper.class, Json.class)).toInstance(TestHelper.makeJsonMapper());

        // This Module is just to get a LookupExtractorFactoryContainerProvider with a usable "lookyloo" lookup.

        /*
        final LookupExtractorFactoryContainerProvider lookupProvider =
            LookupEnabledTestExprMacroTable.createTestLookupProvider(
                ImmutableMap.of(
                    "a", "xa",
                    "abc", "xabc",
                    "nosuchkey", "mysteryvalue",
                    "6", "x6"
                )
            );
        binder.bind(LookupExtractorFactoryContainerProvider.class).toInstance(lookupProvider);
        */
      }
  );

  public static ExprMacroTable createExprMacroTable()
  {
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : EXPR_MACROS) {
      exprMacros.add(INJECTOR.getInstance(clazz));
    }
    exprMacros.add(INJECTOR.getInstance(LookupExprMacro.class));
    return new ExprMacroTable(exprMacros);
  }



  /**
   * This is a provider of query contexts that should be used by join tests.
   * It tests various configs that can be passed to join queries. All the configs provided by this provider should
   * have the join query engine return the same results.
   */
  public static class QueryContextForJoinProvider
  {
    @UsedByJUnitParamsRunner
    public static Object[] provideQueryContexts()
    {
      return new Object[] {
          // default behavior
          QUERY_CONTEXT_DEFAULT,
          // filter value re-writes enabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, true)
              .build(),
          // rewrite values enabled but filter re-writes disabled.
          // This should be drive the same behavior as the previous config
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY, true)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .build(),
          // filter re-writes disabled
          new ImmutableMap.Builder<String, Object>()
              .putAll(QUERY_CONTEXT_DEFAULT)
              .put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, false)
              .build(),
          };
    }
  }
}
