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

package org.apache.druid.segment;

import org.apache.commons.io.FileUtils;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class MemoryBoundedIndexMerger implements IndexMerger
{
  @Override
  public File persist(
      IncrementalIndex index,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    FileUtils.deleteDirectory(outDir);
    FileUtils.forceMkdir(outDir);

    List<IndexableAdapter> adapters = IndexMerger.toIndexableAdapters(indexes);

    final List<String> mergedDimensions = IndexMerger.getMergedDimensions(adapters);

    final List<String> mergedMetrics = IndexMerger.mergeIndexed(
        adapters.stream().map(IndexableAdapter::getMetricNames).collect(Collectors.toList())
    );

    final AggregatorFactory[] sortedMetricAggs = new AggregatorFactory[mergedMetrics.size()];
    for (AggregatorFactory metricAgg : metricAggs) {
      int metricIndex = mergedMetrics.indexOf(metricAgg.getName());
      /*
        If metricIndex is negative, one of the metricAggs was not present in the union of metrics from the indices
        we are merging
       */
      if (metricIndex > -1) {
        sortedMetricAggs[metricIndex] = metricAgg;
      }
    }


    return null;
  }

  @Override
  public File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File merge(
      List<IndexableAdapter> indexes, boolean rollup, AggregatorFactory[] metricAggs, File outDir, IndexSpec indexSpec
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File convert(File inDir, File outDir, IndexSpec indexSpec) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
  ) throws IOException
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
