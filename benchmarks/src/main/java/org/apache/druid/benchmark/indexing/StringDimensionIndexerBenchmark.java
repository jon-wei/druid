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

package org.apache.druid.benchmark.indexing;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.StringDimensionIndexer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class StringDimensionIndexerBenchmark
{
  StringDimensionIndexer indexer;
  int[] exampleArray;
  List<Object> rowVals;

  @Param({"100000"})
  public int cardinality;

  @Param({"1"})
  public int rowSize;

  @Setup
  public void setup()
  {
    rowVals = new ArrayList<>();
    indexer = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true);

    for (int i = 0; i < cardinality; i++) {
      String val = "abcd-" + i;
      indexer.processRowValsToUnsortedEncodedKeyComponent(val, true);
      rowVals.add(val);
    }

    exampleArray = new int[rowSize];
    int stride = cardinality / rowSize;
    for (int i = 0; i < rowSize; i++) {
      exampleArray[i] = i * stride;
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void estimateEncodedKeyComponentSize(Blackhole blackhole)
  {
    long sz = indexer.estimateEncodedKeyComponentSize(exampleArray);
    blackhole.consume(sz);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void processRowValsToUnsortedEncodedKeyComponent(Blackhole blackhole)
  {
    StringDimensionIndexer indexer2 = new StringDimensionIndexer(DimensionSchema.MultiValueHandling.ofDefault(), true);
    for (Object rowVal : rowVals) {
      int[] rowVals = indexer2.processRowValsToUnsortedEncodedKeyComponent(rowVal, true);
      blackhole.consume(rowVals);
    }
  }
}
