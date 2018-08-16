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

package org.apache.druid.benchmark;

import org.apache.commons.math3.analysis.UnivariateFunction;
import org.apache.commons.math3.analysis.integration.BaseAbstractUnivariateIntegrator;
import org.apache.commons.math3.analysis.integration.IterativeLegendreGaussIntegrator;
import org.apache.commons.math3.analysis.integration.RombergIntegrator;
import org.apache.commons.math3.analysis.integration.SimpsonIntegrator;
import org.apache.commons.math3.analysis.integration.TrapezoidIntegrator;
import org.apache.commons.math3.analysis.integration.UnivariateIntegrator;
import org.apache.commons.math3.analysis.interpolation.SplineInterpolator;
import org.apache.commons.math3.analysis.interpolation.UnivariateInterpolator;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogram;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
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

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class RebinningBenchmark
{
  // Number of sequences to merge at once
  @Param({"100"})
  int numPoints;

  // Number of sequences to merge at once
  @Param({"67"})
  int newBuckets;

  private UnivariateInterpolator interpolator;
  private double[] x;
  private double[] y;
  private UnivariateFunction savedFunction;
  private UnivariateIntegrator legendreGauss;
  private UnivariateIntegrator trapezoid;
  private UnivariateIntegrator simpson;
  private UnivariateIntegrator romberg;
  private FixedBucketsHistogram h;
  private ApproximateHistogram ah;

  private int[] randoms;

  private FixedBucketsHistogram buildHistogram(
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

  private ApproximateHistogram buildApproxHistogram(
      float[] values
  )
  {
    //ApproximateHistogram ah = new ApproximateHistogram(1000, 0, 100);
    ApproximateHistogram ah = new ApproximateHistogram(100, 0, 10000);

    for (float v : values) {
      ah.offer(v);
    }
    return ah;
  }

  @Setup
  public void setup() throws Exception
  {
    Random rng = new Random(9999);
    interpolator = new SplineInterpolator();
    x = new double[numPoints];
    y = new double[numPoints];

    for (int i = 0; i < numPoints; i++) {
      x[i] = i;
      y[i] = rng.nextDouble() * 1000;
    }

    savedFunction = interpolator.interpolate(x, y);
    trapezoid = new TrapezoidIntegrator();
    simpson = new SimpsonIntegrator();
    romberg = new RombergIntegrator();
    legendreGauss = new IterativeLegendreGaussIntegrator(
        5,
        BaseAbstractUnivariateIntegrator.DEFAULT_RELATIVE_ACCURACY,
        BaseAbstractUnivariateIntegrator.DEFAULT_ABSOLUTE_ACCURACY
    );

    h = buildHistogram(
        0,
        10000,
        100,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        new float[]{10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 60, 60, 70, 70, 80, 80, 90, 90, 99, 99}
    );

    ah = buildApproxHistogram(
        new float[]{10, 10, 20, 20, 30, 30, 40, 40, 50, 50, 60, 60, 70, 70, 80, 80, 90, 90, 99, 99}
    );

    randoms = new int[100000];
    Random r = ThreadLocalRandom.current();
    for (int i = 0; i < 100000; i++) {
      randoms[i] = r.nextInt(1000);
    }
  }

  @Benchmark
  public void interpolate(Blackhole bh)
  {
    UnivariateFunction function = interpolator.interpolate(x, y);
    bh.consume(function);
  }

  @Benchmark
  public void rebinTrapezoid(Blackhole bh)
  {
    double[] newHistogram = new double[newBuckets];
    double newBucketSize = ((double) numPoints) / newBuckets;
    for (int i = 0; i < newBuckets - 1; i++) {
      newHistogram[i] = trapezoid.integrate(10000, savedFunction, i * newBucketSize, (i + 1) * newBucketSize);
    }
    bh.consume(newHistogram);
  }

  @Benchmark
  public void rebinSimpson(Blackhole bh)
  {
    double[] newHistogram = new double[newBuckets];
    double newBucketSize = ((double) numPoints) / newBuckets;
    for (int i = 0; i < newBuckets - 1; i++) {
      newHistogram[i] = simpson.integrate(10000, savedFunction, i * newBucketSize, (i + 1) * newBucketSize);
    }
    bh.consume(newHistogram);
  }

  @Benchmark
  public void rebinRomberg(Blackhole bh)
  {
    double[] newHistogram = new double[newBuckets];
    double newBucketSize = ((double) numPoints) / newBuckets;
    for (int i = 0; i < newBuckets - 1; i++) {
      newHistogram[i] = romberg.integrate(10000, savedFunction, i * newBucketSize, (i + 1) * newBucketSize);
    }
    bh.consume(newHistogram);
  }

  @Benchmark
  public void rebinGauss(Blackhole bh)
  {
    double[] newHistogram = new double[newBuckets];
    double newBucketSize = ((double) numPoints) / newBuckets;
    for (int i = 0; i < newBuckets - 1; i++) {
      newHistogram[i] = legendreGauss.integrate(10000, savedFunction, i * newBucketSize, (i + 1) * newBucketSize);
    }
    bh.consume(newHistogram);
  }

  @Benchmark
  public void serializeFullHisto(Blackhole bh)
  {
    byte[] bs = h.toBytesFull();
    //System.out.println("full.size: " + bs.length);
    bh.consume(bs);
  }

  @Benchmark
  public void serializeLZ4Histo(Blackhole bh)
  {
    for (int i = 0; i < 100000; i++) {
      h.add(randoms[i]);
    }
    byte[] bs = h.toBytesLZ4();
    //ystem.out.println("lz4.size: " + bs.length);
    bh.consume(bs);
  }

  @Benchmark
  public void serializeApproxHisto(Blackhole bh)
  {
    for (int i = 0; i < 100000; i++) {
      ah.offer(randoms[i]);
    }
    byte[] bs = ah.toBytes();
    //System.out.println("ah.size: " + bs.length);
    bh.consume(bs);
  }

  @Benchmark
  public void addApproxHisto(Blackhole bh)
  {
    for (int i = 0; i < 100000; i++) {
      ah.offer(randoms[i]);
    }
    bh.consume(ah);
  }

  @Benchmark
  public void addFixedHisto(Blackhole bh)
  {
    for (int i = 0; i < 100000; i++) {
      h.add(randoms[i]);
    }
    bh.consume(h);
  }
}
