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

package org.apache.druid.query.aggregation.histogram;

import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.Assert;
import org.junit.Test;

public class FixedBucketsHistogramTest
{
  private static final Logger log = new Logger(FixedBucketsHistogramTest.class);

  static final float[] VALUES = {23, 19, 10, 16, 36, 2, 9, 32, 30, 45};
  static final float[] VALUES2 = {23, 19, 10, 16, 36, 2, 1, 9, 32, 30, 45, 46};

  static final float[] VALUES3 = {
      20, 16, 19, 27, 17, 20, 18, 20, 28, 14, 17, 21, 20, 21, 10, 25, 23, 17, 21, 18,
      14, 20, 18, 12, 19, 20, 23, 25, 15, 22, 14, 17, 15, 23, 23, 15, 27, 20, 17, 15
  };
  static final float[] VALUES4 = {
      27.489f, 3.085f, 3.722f, 66.875f, 30.998f, -8.193f, 5.395f, 5.109f, 10.944f, 54.75f,
      14.092f, 15.604f, 52.856f, 66.034f, 22.004f, -14.682f, -50.985f, 2.872f, 61.013f,
      -21.766f, 19.172f, 62.882f, 33.537f, 21.081f, 67.115f, 44.789f, 64.1f, 20.911f,
      -6.553f, 2.178f
  };
  static final float[] VALUES5 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  static final float[] VALUES6 = {
      1f, 1.5f, 2f, 2.5f, 3f, 3.5f, 4f, 4.5f, 5f, 5.5f, 6f, 6.5f, 7f, 7.5f, 8f, 8.5f, 9f, 9.5f, 10f
  };

  // Based on the example from https://metamarkets.com/2013/histograms/
  // This dataset can make getQuantiles() return values exceeding max
  // for example: q=0.95 returns 25.16 when max=25
  static final float[] VALUES7 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 12, 12, 15, 20, 25, 25, 25};

  protected FixedBucketsHistogram buildHistogram(
      double lowerLimit,
      double upperLimit,
      int numBuckets,
      FixedBucketsHistogram.OutlierHandlingMode outlierHandlingMode,
      float[] values
  )
  {
    FixedBucketsHistogram h = new FixedBucketsHistogram(
        lowerLimit,
        upperLimit,
        numBuckets,
        outlierHandlingMode
    );

    for (float v : values) {
      h.add(v);
    }
    return h;
  }

  @Test
  public void testOffer()
  {
    log.info("testOffer");
    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES2.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES2[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferWithNegatives()
  {
    log.info("testOfferWithNegative");
    FixedBucketsHistogram h = buildHistogram(
        -100,
        100,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES2
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES2.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES2[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues3()
  {
    log.info("testOfferValues3");

    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES3
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES3.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES3[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues5()
  {
    log.info("testOfferValues5");

    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES5
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES5.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES5[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues6()
  {
    log.info("testOfferValues6");

    FixedBucketsHistogram h = buildHistogram(
        0,
        10,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES6
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES6.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES6[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testOfferValues7()
  {
    log.info("testOfferValues7");

    FixedBucketsHistogram h = buildHistogram(
        0,
        50,
        50,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        VALUES7
    );

    float[] quantiles = h.percentilesFloat(new double[]{12.5f, 50.0f, 98f});
    double[] doubles = new double[VALUES7.length];

    for (int i = 0; i < doubles.length; i++) {
      doubles[i] = VALUES7[i];
    }

    Percentile percentile = new Percentile();
    percentile.setData(doubles);
    log.info("MY-P12.5: " + quantiles[0]);
    log.info("MY-P50: " + quantiles[1]);
    log.info("MY-P98: " + quantiles[2]);
    log.info("THEIR-P12.5: " + percentile.evaluate(12.5));
    log.info("THEIR-P50: " + percentile.evaluate(50));
    log.info("THEIR-P98: " + percentile.evaluate(98));
  }

  @Test
  public void testMergeSameBuckets()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1,2,7,12,19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{3,8,9,13}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(h.getNumBuckets(), 5);
    Assert.assertEquals(h.getBucketSize(), 4.0, 0.01);
    Assert.assertEquals(h.getLowerLimit(), 0, 0.01);
    Assert.assertEquals(h.getUpperLimit(), 20, 0.01);
    Assert.assertEquals(h.getOutlierHandlingMode(), FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW);
    Assert.assertArrayEquals(h.getHistogram(), new long[]{3, 1, 2, 2, 1});
    Assert.assertEquals(h.getCount(), 9);
    Assert.assertEquals(h.getMin(), 1, 0.01);
    Assert.assertEquals(h.getMax(), 19, 0.01);
    Assert.assertEquals(h.getMissingValueCount(), 0);
    Assert.assertEquals(h.getLowerOutlierCount(), 0);
    Assert.assertEquals(h.getUpperOutlierCount(), 0);
  }


  @Test
  public void testMergeSameBucketsRightOverlap()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        20,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{1, 2, 7, 12, 19}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        12,
        32,
        5,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{13, 18, 25, 29}
    );

    h.combineHistogram(h2);

    Assert.assertEquals(h.getNumBuckets(), 5);
    Assert.assertEquals(h.getBucketSize(), 4.0, 0.01);
    Assert.assertEquals(h.getLowerLimit(), 0, 0.01);
    Assert.assertEquals(h.getUpperLimit(), 20, 0.01);
    Assert.assertEquals(h.getOutlierHandlingMode(), FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW);
    Assert.assertArrayEquals(h.getHistogram(), new long[]{2, 1, 0, 2, 2});
    Assert.assertEquals(h.getCount(), 7);
    Assert.assertEquals(h.getMin(), 1, 0.01);
    Assert.assertEquals(h.getMax(), 19, 0.01);
    Assert.assertEquals(h.getMissingValueCount(), 0);
    Assert.assertEquals(h.getLowerOutlierCount(), 0);
    Assert.assertEquals(h.getUpperOutlierCount(), 0);
  }


  /*
  @Test
  public void testOverlapMerge()
  {
    FixedBucketsHistogram h = buildHistogram(
        0,
        200,
        20,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    FixedBucketsHistogram h2 = buildHistogram(
        0,
        100,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        //new float[]{10,20,30,40,50,60,70,80,90,99}
        new float[]{10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 60, 60, 70, 70, 80, 80, 90, 90, 99, 99}
    );

    FixedBucketsHistogram h4 = buildHistogram(
        37,
        96,
        17,
        FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
        //new float[]{10,20,30,40,50,60,70,80,90,99}
        new float[]{40, 40, 50, 50, 60, 60, 70, 70, 80, 80, 90, 90}
    );

    FixedBucketsHistogram hh = buildHistogram(
        0,
        200,
        20,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    //hh.combineHistogram(h2);

    hh.combineHistogram(h4);

    /*
    h.combineHistogram(h2);

    byte[] lz4d = h.toBytesLZ4();
    FixedBucketsHistogram hu = FixedBucketsHistogram.fromBytes(lz4d);

    String b64 = FixedBucketsHistogram.toBase64(h);
    log.info(b64);
    FixedBucketsHistogram h4 = FixedBucketsHistogram.fromBase64(b64);
    log.info("S");
    */

  /*
    h = FixedBucketsHistogram.fromBase64("AQAAAOgTAAEAI0BpCQAwAAAUBQADAgAIDAAJAgAxQFjAEAAhQCQHAAwCABMCEQAPCAAlEwRAAA8CADFQAAAAAAA=");

    FixedBucketsHistogram h3 = buildHistogram(
        0,
        100,
        10,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{}
    );

    h3.combineHistogram(h);
    log.info("DFSFSDFDSF");
  }
  */
}
