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

package org.apache.druid.query.topn;

import com.google.common.io.CharSource;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class MultiSegmentTopNQueryTest
{
  private static final TopNQueryQueryToolChest toolChest = new TopNQueryQueryToolChest(
      new TopNQueryConfig(),
      QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
  );

  private static final QueryRunnerFactory factory = new TopNQueryRunnerFactory(
      TestQueryRunners.createDefaultNonBlockingPool(),
      toolChest,
      QueryRunnerTestHelper.NOOP_QUERYWATCHER
  );

  // time modified version of druid.sample.numeric.tsv
  public static final String[] V_0112 = {
      "2011-01-12T00:00:00.000Z\tspot\tautomotive\t1000\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t100.000000"
  };

  public static final String[] V_0113 = {
      "2011-01-13T00:00:00.000Z\tspot\tautomotive\t8147483647\t10000.0\t10000.0\t100000\tpreferred\tapreferred\t100.000000"
  };

  private static Segment segment0;
  private static Segment segment1;

  @BeforeClass
  public static void setup() throws IOException
  {
    CharSource v_0112 = CharSource.wrap(StringUtils.join(V_0112, "\n"));
    CharSource v_0113 = CharSource.wrap(StringUtils.join(V_0113, "\n"));

    IncrementalIndex index0 = TestIndex.loadIncrementalIndex(newIndex("2011-01-12T00:00:00.000Z"), v_0112);
    IncrementalIndex index1 = TestIndex.loadIncrementalIndex(newIndex("2011-01-13T00:00:00.000Z"), v_0113);

    segment0 = new IncrementalIndexSegment(index0, makeIdentifier(index0, "v1"));
    segment1 = new IncrementalIndexSegment(index1, makeIdentifier(index1, "v1"));
  }

  private static SegmentId makeIdentifier(IncrementalIndex index, String version)
  {
    return makeIdentifier(index.getInterval(), version);
  }

  private static SegmentId makeIdentifier(Interval interval, String version)
  {
    return SegmentId.of(QueryRunnerTestHelper.dataSource, interval, version, NoneShardSpec.instance());
  }

  private static IncrementalIndex newIndex(String minTimeStamp)
  {
    return newIndex(minTimeStamp, 10000);
  }

  private static IncrementalIndex newIndex(String minTimeStamp, int maxRowCount)
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withMinTimestamp(DateTimes.of(minTimeStamp).getMillis())
        .withQueryGranularity(Granularities.HOUR)
        .withMetrics(TestIndex.METRIC_AGGS)
        .build();
    return new IncrementalIndex.Builder()
        .setIndexSchema(schema)
        .setMaxRowCount(maxRowCount)
        .buildOnheap();
  }

  @AfterClass
  public static void clear()
  {
    IOUtils.closeQuietly(segment0);
    IOUtils.closeQuietly(segment1);
  }

  @Test
  public void testLongDimMerge()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityLong", "qualityLong", ValueType.LONG))
        .metric("hello")
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
        .aggregators(
            Collections.singletonList(
                new LongSumAggregatorFactory("hello", "index")
            )
        )
        .build();

    List<QueryRunner<Result<TopNResultValue>>> singleSegmentRunners = new ArrayList<>();
    QueryRunner<Result<TopNResultValue>> runner0 = factory.createRunner(segment0);
    QueryRunner<Result<TopNResultValue>> runner1 = factory.createRunner(segment1);
    singleSegmentRunners.add(runner0);
    singleSegmentRunners.add(runner1);

    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            factory.getToolchest().mergeResults(factory.mergeRunners(Execs.directExecutor(), singleSegmentRunners)),
            factory.getToolchest()
        )
    );
    Sequence<Result<TopNResultValue>> queryResult = theRunner.run(
        QueryPlus.wrap(query),
        ResponseContext.createEmpty()
    );
    List<Result<TopNResultValue>> results = queryResult.toList();

    int totalCount = 0;
    for (Result<TopNResultValue> result : results) {
      System.out.println(result.getValue());
    }
  }
}

/***
 * {
 "queryType": "topN",
 "dataSource": {
 "type": "table",
 "name": "topnmerge"
 },
 "virtualColumns": [],
 "dimension": {
 "type": "default",
 "dimension": "id",
 "outputName": "d0",
 "outputType": "LONG"
 },
 "metric": {
 "type": "numeric",
 "metric": "a0"
 },
 "threshold": 1000,
 "intervals": {
 "type": "intervals",
 "intervals": [
 "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
 ]
 },
 "filter": null,
 "granularity": {
 "type": "all"
 },
 "aggregations": [
 {
 "type": "longSum",
 "name": "a0",
 "fieldName": "val",
 "expression": null
 }
 ],
 "postAggregations": [],
 "context": {
 "sqlQueryId": "7ed006df-9ac4-4339-aa96-df3d39dab4d5"
 },
 "descending": false
 }
*/
