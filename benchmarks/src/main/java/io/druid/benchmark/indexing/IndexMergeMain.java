package io.druid.benchmark.indexing;

import io.druid.segment.QueryableIndex;

import java.util.List;

/**
 * Created by jw on 6/28/19.
 */
public class IndexMergeMain
{
  public static void main(String [] args) throws Exception
  {
    IMBenchmarker imBenchmarker = new IMBenchmarker();
    //imBenchmarker.setup();

    List<QueryableIndex> indexesToMerge = imBenchmarker.loadQueryableIndexes();
    imBenchmarker.runBenchmark(indexesToMerge, 50);
  }
}
