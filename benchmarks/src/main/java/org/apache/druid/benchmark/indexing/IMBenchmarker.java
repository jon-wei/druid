package org.apache.druid.benchmark.indexing;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.hll.HyperLogLogHash;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IMBenchmarker
{
  private int numSegments = 5;

  private int rowsPerSegment = 75000;

  private String schema = "basic";

  private static final Logger log = new Logger(IndexMergeBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  //private List<QueryableIndex> indexesToMerge;
  private BenchmarkSchemaInfo schemaInfo;
  private File tmpDir;
  private File tmpDir2;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    InjectableValues.Std injectableValues = new InjectableValues.Std();
    injectableValues.addValue(ExprMacroTable.class, ExprMacroTable.nil());
    JSON_MAPPER.setInjectableValues(injectableValues);
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(true)
                .build()
        )
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
  }

  public List<QueryableIndex> loadQueryableIndexes() throws Exception
  {
    List<QueryableIndex> queryableIndices = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      File segment = new File("/tmp/merge/" + i);
      QueryableIndex queryableIndex = INDEX_IO.loadIndex(segment);
      queryableIndices.add(queryableIndex);
    }
    return queryableIndices;
  }

  public void runBenchmark(List<QueryableIndex> indexesToMerge, int iterations) throws Exception
  {
    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    for (int i = 0; i < iterations; i++) {
      File tmpFile = File.createTempFile(
          "IndexMergeBenchmark-MERGEDFILE-V9-" + System.currentTimeMillis(),
          ".TEMPFILE"
      );
      tmpFile.delete();
      tmpFile.mkdirs();
      try {
        System.out.println(tmpFile.getAbsolutePath()
                           + " isFile: "
                           + tmpFile.isFile()
                           + " isDir:"
                           + tmpFile.isDirectory());

        long start = System.currentTimeMillis();
        File mergedFile = INDEX_MERGER_V9.mergeQueryableIndex(
            indexesToMerge,
            true,
            schemaInfo.getAggsArray(),
            tmpFile,
            new IndexSpec(),
            null
        );
        long end = System.currentTimeMillis();
        long diff = end - start;

        System.out.println("RUN: " + diff);
      }
      finally {
        tmpFile.delete();
      }
    }
  }

  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ComplexMetrics.registerSerde("hyperUnique", () -> new HyperUniquesSerde(HyperLogLogHash.getDefault()));

    //indexesToMerge = new ArrayList<>();

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schema);

    for (int i = 0; i < numSegments; i++) {
      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }

      tmpDir = new File("/tmp/merge/" + i);
      tmpDir.mkdir();
      tmpDir2 = Files.createTempDir();
      log.info("Using temp dir: " + tmpDir2.getAbsolutePath());

      File indexFile = INDEX_MERGER_V9.persist(
          incIndex,
          tmpDir,
          new IndexSpec(),
          null
      );

      System.out.println("SEGMENT: " + indexFile.getAbsolutePath());

      //QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      //indexesToMerge.add(qIndex);
    }
  }
}
