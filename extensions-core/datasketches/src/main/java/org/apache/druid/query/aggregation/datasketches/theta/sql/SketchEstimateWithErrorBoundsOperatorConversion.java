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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.SketchEstimatePostAggregator;
import org.apache.druid.sql.calcite.expression.DirectOperatorConversion;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.OperatorConversions;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class SketchEstimateWithErrorBoundsOperatorConversion extends DirectOperatorConversion
{
  private static String FUNCTION_NAME = "theta_sketch_estimate_with_error_bounds";
  private static final SqlFunction SQL_FUNCTION = OperatorConversions
      .operatorBuilder(StringUtils.toUpperCase(FUNCTION_NAME))
      .operandTypes(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER)
      .returnType(SqlTypeName.OTHER)
      .build();


  public SketchEstimateWithErrorBoundsOperatorConversion()
  {
    super(SQL_FUNCTION, FUNCTION_NAME);
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return SQL_FUNCTION;
  }

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
    final PostAggregator firstOperand = OperatorConversions.toPostAggregator(
        plannerContext,
        rowSignature,
        operands.get(0),
        outputNamePrefix,
        outputNameCounter
    );

    if (firstOperand == null) {
      return null;
    }

    final int errorBoundsStdDev =  ((Number) RexLiteral.value(operands.get(1))).intValue();

    return new SketchEstimatePostAggregator(
        outputNamePrefix + outputNameCounter.getAndIncrement(),
        firstOperand,
        errorBoundsStdDev
    );
  }
}
