package org.apache.druid.benchmark.indexing;

import org.apache.druid.segment.QueryableIndex;

import java.util.List;

public class IndexMergeMain
{
  public static void main(String [] args) throws Exception
  {
    IMBenchmarker imBenchmarker = new IMBenchmarker();
    //imBenchmarker.setup();

    List<QueryableIndex> indexesToMerge = imBenchmarker.loadQueryableIndexes();
    imBenchmarker.runBenchmark(indexesToMerge, 25);
  }
}
