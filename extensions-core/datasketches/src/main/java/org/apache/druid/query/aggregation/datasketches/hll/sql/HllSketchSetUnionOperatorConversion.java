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

package org.apache.druid.query.aggregation.datasketches.hll.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchUnionPostAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.SketchSetPostAggregator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class HllSketchSetUnionOperatorConversion implements SqlOperatorConversion
{
  private static final SqlFunction SQL_FUNCTION = new SqlFunction(
      "HLL_SKETCH_UNION",
      SqlKind.OTHER_FUNCTION,
      ReturnTypes.explicit(
          factory -> Calcites.createSqlType(factory, SqlTypeName.OTHER)
      ),
      null,
      OperandTypes.VARIADIC,
      SqlFunctionCategory.USER_DEFINED_FUNCTION
  );

  public HllSketchSetUnionOperatorConversion()
  {
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode
  )
  {
    return null;
  }

  @Nullable
  @Override
  public PostAggregator toPostAggregator(
      PlannerContext plannerContext,
      RowSignature rowSignature,
      RexNode rexNode,
      final String outputNamePrefix,
      final AtomicInteger outputNameCounter
  )
  {
    final List<RexNode> operands = ((RexCall) rexNode).getOperands();
    final List<PostAggregator> inputPostAggs = new ArrayList<>();
    Integer lgK = null;
    String tgtHllType = null;

    int operandCounter = 0;
    for (RexNode operand : operands) {
      final PostAggregator convertedPostAgg = OperatorConversions.toPostAggregator(
          plannerContext,
          rowSignature,
          operand,
          outputNamePrefix,
          outputNameCounter
      );
      if (convertedPostAgg == null) {
        if (operandCounter == 0) {
          try {
            lgK = RexLiteral.intValue(operand);
          } catch (RuntimeException re) {
            return null;
          }
        } else if (operandCounter == 1) {
          try {
            tgtHllType = RexLiteral.stringValue(operand);
          } catch (RuntimeException re) {
            return null;
          }
        } else {
          return null;
        }
      } else {
        inputPostAggs.add(convertedPostAgg);
        operandCounter++;
      }
    }

    return new HllSketchUnionPostAggregator(
        outputNamePrefix + outputNameCounter.getAndIncrement(),
        inputPostAggs,
        lgK,
        tgtHllType
    );
  }
}
