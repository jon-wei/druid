package org.apache.druid.benchmark.indexing;

import org.apache.druid.java.util.common.logger.Logger;

public class IndexMergeMain
{
  private static final Logger log = new Logger(IndexMergeMain.class);

  public static void main(String[] args) throws Exception
  {
    IndexMergeBenchmark imb = new IndexMergeBenchmark();
    //log.info("STARTING SETUP1");
    //imb.setup();
    log.info("STARTING SETUP2");
    imb.setup2();
    log.info("STARTING MERGE");
    imb.mergeV9test();
  }
}
