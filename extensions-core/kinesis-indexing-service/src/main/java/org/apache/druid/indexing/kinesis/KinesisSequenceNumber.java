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

package org.apache.druid.indexing.kinesis;


import org.apache.druid.indexing.seekablestream.common.OrderedSequenceNumber;

import java.math.BigInteger;

// OrderedSequenceNumber.equals() should be used instead.
@SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
public class KinesisSequenceNumber extends OrderedSequenceNumber<String>
{
  /**
   * In Kinesis, when a shard is closed due to shard splitting, a null ShardIterator is returned.
   * The EOS marker is placed at the end of the Kinesis Record Supplier buffer, such that when
   * an indexing task pulls the record 'EOS', it knows the shard has been closed and should stop
   * reading and start publishing
   */
  public static final String END_OF_SHARD_MARKER = "EOS";

  // this special marker is used by the KinesisSupervisor to set the endOffsets
  // of newly created indexing tasks. This is necessary because streaming tasks do not
  // have endPartitionOffsets. This marker signals to the task that it should continue
  // to ingest data until taskDuration has elapsed or the task was stopped or paused or killed
  public static final String NO_END_SEQUENCE_NUMBER = "NO_END_SEQUENCE_NUMBER";

  // this flag is used to indicate either END_OF_SHARD_MARKER
  // or NO_END_SEQUENCE_NUMBER so that they can be properly compared
  // with other sequence numbers
  //private final boolean isMaxSequenceNumber;

  private final boolean isEndOfShardMarker;
  private final boolean isNoEndSequenceNumber;

  private final BigInteger intSequence;

  private KinesisSequenceNumber(String sequenceNumber, boolean isExclusive)
  {
    super(sequenceNumber, isExclusive);

    if (END_OF_SHARD_MARKER.equals(sequenceNumber)) {
      this.isEndOfShardMarker = true;
      this.isNoEndSequenceNumber = false;
      this.intSequence = null;
    } else if (NO_END_SEQUENCE_NUMBER.equals(sequenceNumber)) {
      this.isEndOfShardMarker = false;
      this.isNoEndSequenceNumber = true;
      this.intSequence = null;
    } else {
      this.isEndOfShardMarker = false;
      this.isNoEndSequenceNumber = false;
      this.intSequence = new BigInteger(sequenceNumber);
    }
  }

  public static KinesisSequenceNumber of(String sequenceNumber)
  {
    return new KinesisSequenceNumber(sequenceNumber, false);
  }

  public static KinesisSequenceNumber of(String sequenceNumber, boolean isExclusive)
  {
    return new KinesisSequenceNumber(sequenceNumber, isExclusive);
  }

  @Override
  public int compareTo(OrderedSequenceNumber<String> o)
  {
    KinesisSequenceNumber num = (KinesisSequenceNumber) o;

    if (isEndOfShardMarker) {
      if (num.isEndOfShardMarker) {
        return 0;
      } else {
        // EOS is less than anything else
        return -1;
      }
    }

    if (isNoEndSequenceNumber) {
      if (num.isNoEndSequenceNumber) {
        return 0;
      } else {
        // no end is greater than anything else
        return 1;
      }
    }

    if (num.isEndOfShardMarker) {
      // numbered offset is greater than EOS
      return 1;
    }

    if (num.isNoEndSequenceNumber) {
      // numbered offset is less than NO_END
      return -1;
    }

    return this.intSequence.compareTo(new BigInteger(o.get()));
  }
}
