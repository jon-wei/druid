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

import org.apache.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.selector.settable.SettableColumnValueSelector;
import org.apache.druid.segment.selector.settable.SettableDimensionValueSelector;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.util.Comparator;

public class StringDimensionHandler implements DimensionHandler<Integer, int[], String>
{
  /**
   * Compares {@link IndexedInts}, first checking the sizes of the value arrays. The shorter array is considered
   * smaller if the sizes differ.
   *
   * If the sizes are equal, then the method walks each array until the value at a given index differs, and
   * that comparison result is returned.
   *
   * The comparison logic in this method must be kept in sync with
   * {@link StringDimensionIndexer#compareUnsortedEncodedKeyComponents}, since this comparator is used to order rows
   * when merging segments. If the comparison logic does not match, imperfect rollup during segment merging can occur.
   */
  private static final Comparator<ColumnValueSelector> DIMENSION_SELECTOR_COMPARATOR = (s1, s2) -> {
    IndexedInts row1 = getRow(s1);
    IndexedInts row2 = getRow(s2);
    int len1 = row1.size();
    int len2 = row2.size();
    int retVal = Integer.compare(len1, len2);
    int valsIndex = 0;
    while (retVal == 0 && valsIndex < len1) {
      int lhsVal = row1.get(valsIndex);
      int rhsVal = row2.get(valsIndex);
      retVal = Integer.compare(lhsVal, rhsVal);
      ++valsIndex;
    }
    return retVal;
  };

  /**
   * Value for absent column, i. e. {@link NilColumnValueSelector}, should be equivalent to [null] during index merging.
   *
   * During index merging, if one of the merged indexes has absent columns, {@link StringDimensionMergerV9} ensures
   * that null value is present, and it has index = 0 after sorting, because sorting puts null first. See {@link
   * StringDimensionMergerV9#hasNull} and the place where it is assigned.
   */
  private static IndexedInts getRow(ColumnValueSelector s)
  {
    if (s instanceof DimensionSelector) {
      return ((DimensionSelector) s).getRow();
    } else if (s instanceof NilColumnValueSelector) {
      return ZeroIndexedInts.instance();
    } else {
      throw new ISE(
          "ColumnValueSelector[%s], only DimensionSelector or NilColumnValueSelector is supported",
          s.getClass()
      );
    }
  }

  private final String dimensionName;
  private final MultiValueHandling multiValueHandling;
  private final boolean hasBitmapIndexes;

  public StringDimensionHandler(String dimensionName, MultiValueHandling multiValueHandling, boolean hasBitmapIndexes)
  {
    this.dimensionName = dimensionName;
    this.multiValueHandling = multiValueHandling;
    this.hasBitmapIndexes = hasBitmapIndexes;
  }

  @Override
  public String getDimensionName()
  {
    return dimensionName;
  }

  @Override
  public MultiValueHandling getMultivalueHandling()
  {
    return multiValueHandling;
  }

  @Override
  public int getLengthOfEncodedKeyComponent(int[] dimVals)
  {
    return dimVals.length;
  }

  @Override
  public Comparator<ColumnValueSelector> getEncodedValueSelectorComparator()
  {
    return DIMENSION_SELECTOR_COMPARATOR;
  }

  @Override
  public SettableColumnValueSelector makeNewSettableEncodedValueSelector()
  {
    return new SettableDimensionValueSelector();
  }

  @Override
  public DimensionIndexer<Integer, int[], String> makeIndexer()
  {
    return new StringDimensionIndexer(multiValueHandling, hasBitmapIndexes);
  }

  @Override
  public DimensionMergerV9 makeMerger(
      IndexSpec indexSpec,
      SegmentWriteOutMedium segmentWriteOutMedium,
      ColumnCapabilities capabilities,
      ProgressIndicator progress,
      Closer closer
  )
  {
    // Sanity-check capabilities.
    if (hasBitmapIndexes != capabilities.hasBitmapIndexes()) {
      throw new ISE(
          "capabilities.hasBitmapIndexes[%s] != this.hasBitmapIndexes[%s]",
          capabilities.hasBitmapIndexes(),
          hasBitmapIndexes
      );
    }

    return new StringDimensionMergerV9(dimensionName, indexSpec, segmentWriteOutMedium, capabilities, progress, closer);
  }
}
