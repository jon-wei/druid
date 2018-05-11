/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation;

import io.druid.query.PerSegmentQueryOptimizationContext;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

public class SuppressedAggregatorFactory extends AggregatorFactory
{
  private final AggregatorFactory delegate;

  public SuppressedAggregatorFactory(
      AggregatorFactory delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new SuppressedAggregator(delegate.factorize(metricFactory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new SuppressedBufferAggregator(delegate.factorizeBuffered(metricFactory));
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegate.combine(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return delegate.makeAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    return delegate.getMergingFactory(other);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return delegate.finalizeComputation(object);
  }

  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize();
  }

  @Override
  public AggregatorFactory optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    // we are already the result of an optimizeForSegment() call
    return this;
  }

  @Override
  public int hashCode()
  {
    return delegate.hashCode();
  }

  @Override
  public boolean equals(Object obj)
  {
    return delegate.equals(obj);
  }

  @Override
  public byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }

  public static class SuppressedAggregator implements Aggregator
  {
    private final Aggregator delegate;

    public SuppressedAggregator(
        Aggregator delegate
    )
    {
      this.delegate = delegate;
    }

    @Override
    public void aggregate()
    {
      //no-op
    }

    @Nullable
    @Override
    public Object get()
    {
      return delegate.get();
    }

    @Override
    public float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public long getLong()
    {
      return delegate.getLong();
    }

    @Override
    public double getDouble()
    {
      return delegate.getDouble();
    }

    @Override
    public boolean isNull()
    {
      return delegate.isNull();
    }

    @Override
    public void close()
    {
      delegate.close();
    }
  }

  public static class SuppressedBufferAggregator implements BufferAggregator
  {
    private final BufferAggregator delegate;

    public SuppressedBufferAggregator(
        BufferAggregator delegate
    )
    {
      this.delegate = delegate;
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      delegate.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return delegate.get(buf, position);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position)
    {
      return delegate.getFloat(buf, position);
    }

    @Override
    public long getLong(ByteBuffer buf, int position)
    {
      return delegate.getLong(buf, position);
    }

    @Override
    public double getDouble(ByteBuffer buf, int position)
    {
      return delegate.getDouble(buf, position);
    }

    @Override
    public void close()
    {
      delegate.close();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      delegate.inspectRuntimeShape(inspector);
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
    {
      delegate.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public boolean isNull(ByteBuffer buf, int position)
    {
      return delegate.isNull(buf, position);
    }

    @Override
    public int hashCode()
    {
      return delegate.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      return delegate.equals(obj);
    }

    @Override
    public String toString()
    {
      return delegate.toString();
    }
  }
}
