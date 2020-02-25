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

package org.apache.druid.segment.join;

import com.googlecode.concurrenttrees.radix.ConcurrentRadixTree;
import com.googlecode.concurrenttrees.radix.node.NodeFactory;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharArrayNodeFactory;
import com.googlecode.concurrenttrees.radix.node.concrete.DefaultCharSequenceNodeFactory;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.planning.PreJoinableClause;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.utils.JvmUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility methods for working with {@link Joinable} related classes.
 */
public class Joinables
{
  private static final NodeFactory DEFAULT_CHAR_SEQUENCE_NODE_FACTORY = new DefaultCharSequenceNodeFactory();
  private static Comparator<String> C = (s1, s2) -> {
    if (s1.length() > s2.length()) {
      return -1;
    } else if (s1.length() < s2.length()) {
      return 1;
    } else {
      return 0;
    }
  };

  /**
   * Checks that "prefix" is a valid prefix for a join clause (see {@link JoinableClause#getPrefix()}) and, if so,
   * returns it. Otherwise, throws an exception.
   */
  public static String validatePrefix(@Nullable final String prefix)
  {
    if (prefix == null || prefix.isEmpty()) {
      throw new IAE("Join clause cannot have null or empty prefix");
    } else if (isPrefixedBy(ColumnHolder.TIME_COLUMN_NAME, prefix) || ColumnHolder.TIME_COLUMN_NAME.equals(prefix)) {
      throw new IAE(
          "Join clause cannot have prefix[%s], since it would shadow %s",
          prefix,
          ColumnHolder.TIME_COLUMN_NAME
      );
    } else {
      return prefix;
    }
  }

  public static boolean isPrefixedBy(final String columnName, final String prefix)
  {
    return columnName.startsWith(prefix) && columnName.length() > prefix.length();
  }

  /**
   * Creates a Function that maps base segments to {@link HashJoinSegment} if needed (i.e. if the number of join
   * clauses is > 0). If mapping is not needed, this method will return {@link Function#identity()}.
   *
   * @param clauses            pre-joinable clauses
   * @param joinableFactory    factory for joinables
   * @param cpuTimeAccumulator an accumulator that we will add CPU nanos to; this is part of the function to encourage
   *                           callers to remember to track metrics on CPU time required for creation of Joinables
   */
  public static Function<Segment, Segment> createSegmentMapFn(
      final List<PreJoinableClause> clauses,
      final JoinableFactory joinableFactory,
      final AtomicLong cpuTimeAccumulator,
      final boolean enableFilterPushDown
  )
  {
    return JvmUtils.safeAccumulateThreadCpuTime(
        cpuTimeAccumulator,
        () -> {
          if (clauses.isEmpty()) {
            return Function.identity();
          } else {
            final List<JoinableClause> joinableClauses = createJoinableClauses(clauses, joinableFactory);
            return baseSegment -> new HashJoinSegment(baseSegment, joinableClauses, enableFilterPushDown);
          }
        }
    );
  }

  /**
   * Returns a list of {@link JoinableClause} corresponding to a list of {@link PreJoinableClause}. This will call
   * {@link JoinableFactory#build} on each one and therefore may be an expensive operation.
   */
  private static List<JoinableClause> createJoinableClauses(
      final List<PreJoinableClause> clauses,
      final JoinableFactory joinableFactory
  )
  {
    // Since building a JoinableClause can be expensive, check for prefix conflicts before building
    checkPreJoinableClausesForDuplicatesAndShadowing(clauses);

    return clauses.stream().map(preJoinableClause -> {
      final Optional<Joinable> joinable = joinableFactory.build(
          preJoinableClause.getDataSource(),
          preJoinableClause.getCondition()
      );

      return new JoinableClause(
          preJoinableClause.getPrefix(),
          joinable.orElseThrow(() -> new ISE("dataSource is not joinable: %s", preJoinableClause.getDataSource())),
          preJoinableClause.getJoinType(),
          preJoinableClause.getCondition()
      );
    }).collect(Collectors.toList());
  }

  public static void checkPreJoinableClausesForDuplicatesAndShadowing(
      final List<PreJoinableClause> preJoinableClauses
  )
  {
    List<String> prefixes = new ArrayList<>();
    for (PreJoinableClause clause : preJoinableClauses) {
      prefixes.add(clause.getPrefix());
    }

    checkPrefixesForDuplicatesAndShadowing(prefixes);
  }

  public static void checkPrefixesForDuplicatesAndShadowing(
      final List<String> prefixes
  )
  {
    for (int i = 0; i < prefixes.size(); i++) {
      String prefix1 = prefixes.get(i);
      for (int k = 0; k < prefixes.size(); k++) {
        if (i != k) {
          String otherPrefix = prefixes.get(k);
          if (prefix1.equals(otherPrefix)) {
            throw new IAE("Detected duplicate prefix in join clauses: [%s]", prefix1);
          }

          if (isPrefixedBy(prefix1, otherPrefix)) {
            throw new IAE("Detected conflicting prefixes in join clauses: [%s, %s]", prefix1, otherPrefix);
          }
        }
      }
    }
  }


  public static void checkPrefixesForDuplicatesAndShadowing2(
      final List<String> prefixes
  )
  {
    //List<String> prefixesLongestFirst = new ArrayList<>(prefixes);
    List<String> prefixesLongestFirst = prefixes;
    prefixesLongestFirst.sort(C);

    for (int i = 0; i < prefixesLongestFirst.size(); i++) {
      String prefix1 = prefixesLongestFirst.get(i);
      for (int k = i; k < prefixesLongestFirst.size(); k++) {
        if (i != k) {
          String otherPrefix = prefixesLongestFirst.get(k);
          if (prefix1.equals(otherPrefix)) {
            throw new IAE("Detected duplicate prefix in join clauses: [%s]", prefix1);
          }

          if (isPrefixedBy(prefix1, otherPrefix)) {
            throw new IAE("Detected conflicting prefixes in join clauses: [%s, %s]", prefix1, otherPrefix);
          }
        }
      }
    }
  }

  public static void checkPrefixesForDuplicatesAndShadowingTrie(
      final List<String> prefixes
  )
  {
    //List<String> prefixesLongestFirst = new ArrayList<>(prefixes);
    List<String> prefixesLongestFirst = prefixes;
    prefixesLongestFirst.sort(C);

    ConcurrentRadixTree<String> radixTree = new ConcurrentRadixTree<>(DEFAULT_CHAR_SEQUENCE_NODE_FACTORY);
    for (String prefix : prefixesLongestFirst) {
      if (radixTree.putIfAbsent(prefix, "") != null) {
        throw new IAE("Detected duplicate prefix in join clauses: [%s]", prefix);
      }
      // this returns a lazy iterable
      Iterable<CharSequence> prefixedBy = radixTree.getKeysStartingWith(prefix);
      prefixedBy.forEach(
          (conflictingPrefix) -> {
            // we already checked for duplicates when inserting prefixes into tree
            // if length is identical here, then it's the same entry and not a conflict
            if (prefix.length() != conflictingPrefix.length()) {
              throw new IAE("Detected conflicting prefixes in join clauses: [%s, %s]", prefix, conflictingPrefix);
            }
          }
      );
    }
  }


  public static void checkPrefixesForDuplicatesAndShadowingTrie2(
      final List<String> prefixes
  )
  {
    ConcurrentRadixTree<String> radixTree = new ConcurrentRadixTree<>(DEFAULT_CHAR_SEQUENCE_NODE_FACTORY);
    for (String prefix : prefixes) {
      if (radixTree.putIfAbsent(prefix, "") != null) {
        throw new IAE("Detected duplicate prefix in join clauses: [%s]", prefix);
      }
    }

    for (String prefix : prefixes) {
      // this returns a lazy iterable
      Iterable<CharSequence> prefixedBy = radixTree.getKeysStartingWith(prefix);
      prefixedBy.forEach(
          (conflictingPrefix) -> {
            // we already checked for duplicates when inserting prefixes into tree
            // if length is identical here, then it's the same entry and not a conflict
            if (prefix.length() != conflictingPrefix.length()) {
              throw new IAE("Detected conflicting prefixes in join clauses: [%s, %s]", prefix, conflictingPrefix);
            }
          }
      );
    }
  }
}
