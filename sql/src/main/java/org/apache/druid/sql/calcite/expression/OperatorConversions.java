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

package org.apache.druid.sql.calcite.expression;

import com.google.common.base.Preconditions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.RowSignature;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Utilities for assisting in writing {@link SqlOperatorConversion} implementations.
 */
public class OperatorConversions
{
  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.fromFunctionCall(functionName, druidExpressions)
    );
  }

  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String functionName,
      final Function<List<DruidExpression>, SimpleExtraction> simpleExtractionFunction
  )
  {
    return convertCall(
        plannerContext,
        rowSignature,
        rexNode,
        druidExpressions -> DruidExpression.of(
            simpleExtractionFunction == null ? null : simpleExtractionFunction.apply(druidExpressions),
            DruidExpression.functionCall(functionName, druidExpressions)
        )
    );
  }

  @Nullable
  public static DruidExpression convertCall(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final Function<List<DruidExpression>, DruidExpression> expressionFunction
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressions(
        plannerContext,
        rowSignature,
        call.getOperands()
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.apply(druidExpressions);
  }

  @Nullable
  public static DruidExpression convertCallPostAggs(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final Function<List<DruidExpression>, DruidExpression> expressionFunction,
      String outputNamePrefix,
      AtomicInteger outputNameCounter,
      List<PostAggregator> hackyPostAggList
  )
  {
    final RexCall call = (RexCall) rexNode;

    final List<DruidExpression> druidExpressions = Expressions.toDruidExpressionsWithPostAgg(
        plannerContext,
        rowSignature,
        call.getOperands(),
        outputNamePrefix,
        outputNameCounter,
        hackyPostAggList
    );

    if (druidExpressions == null) {
      return null;
    }

    return expressionFunction.apply(druidExpressions);
  }

  /**
   * Translate a Calcite {@code RexNode} to a Druid PostAggregator
   *
   * @param plannerContext SQL planner context
   * @param rowSignature   signature of the rows to be extracted from
   * @param rexNode        expression meant to be applied on top of the rows
   *
   * @return rexNode referring to fields in rowOrder, or null if not possible
   */
  @Nullable
  public static PostAggregator toPostAggregator(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode,
      final String outputNamePrefix,
      final AtomicInteger outputNameCounter
  )
  {
    final SqlKind kind = rexNode.getKind();
    final SqlTypeName sqlTypeName = rexNode.getType().getSqlTypeName();

    if (kind == SqlKind.INPUT_REF) {
      // Translate field references.
      final RexInputRef ref = (RexInputRef) rexNode;
      final String columnName = rowSignature.getRowOrder().get(ref.getIndex());
      if (columnName == null) {
        throw new ISE("WTF?! PostAgg referred to nonexistent index[%d]", ref.getIndex());
      }

      return new FieldAccessPostAggregator(
          columnName,
          columnName
      );
    } else if (rexNode instanceof RexCall) {
      final SqlOperator operator = ((RexCall) rexNode).getOperator();
      final SqlOperatorConversion conversion = plannerContext.getOperatorTable()
                                                             .lookupOperatorConversion(operator);

      if (conversion == null) {
        return null;
      } else {
        return conversion.toPostAggregator(
            plannerContext,
            rowSignature,
            rexNode,
            outputNamePrefix,
            outputNameCounter
        );
      }
    } else if (kind == SqlKind.LITERAL) {
      return null;
    } else {
      throw new IAE("Unknown rexnode kind: " + kind);
    }
  }

  public static OperatorBuilder operatorBuilder(final String name)
  {
    return new OperatorBuilder(name);
  }

  public static class OperatorBuilder
  {
    private String name;
    private SqlKind kind = SqlKind.OTHER_FUNCTION;
    private SqlReturnTypeInference returnTypeInference;
    private SqlFunctionCategory functionCategory = SqlFunctionCategory.USER_DEFINED_FUNCTION;

    // For operand type checking
    private SqlOperandTypeChecker operandTypeChecker;
    private List<SqlTypeFamily> operandTypes;
    private Integer requiredOperands = null;

    private OperatorBuilder(final String name)
    {
      this.name = Preconditions.checkNotNull(name, "name");
    }

    public OperatorBuilder kind(final SqlKind kind)
    {
      this.kind = kind;
      return this;
    }

    public OperatorBuilder returnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlType(factory, typeName)
      );
      return this;
    }

    public OperatorBuilder nullableReturnType(final SqlTypeName typeName)
    {
      this.returnTypeInference = ReturnTypes.explicit(
          factory -> Calcites.createSqlTypeWithNullability(factory, typeName, true)
      );
      return this;
    }

    public OperatorBuilder returnTypeInference(final SqlReturnTypeInference returnTypeInference)
    {
      this.returnTypeInference = returnTypeInference;
      return this;
    }

    public OperatorBuilder functionCategory(final SqlFunctionCategory functionCategory)
    {
      this.functionCategory = functionCategory;
      return this;
    }

    public OperatorBuilder operandTypeChecker(final SqlOperandTypeChecker operandTypeChecker)
    {
      this.operandTypeChecker = operandTypeChecker;
      return this;
    }

    public OperatorBuilder operandTypes(final SqlTypeFamily... operandTypes)
    {
      this.operandTypes = Arrays.asList(operandTypes);
      return this;
    }

    public OperatorBuilder requiredOperands(final int requiredOperands)
    {
      this.requiredOperands = requiredOperands;
      return this;
    }

    public SqlFunction build()
    {
      final SqlOperandTypeChecker theOperandTypeChecker;

      if (operandTypeChecker == null) {
        theOperandTypeChecker = OperandTypes.family(
            Preconditions.checkNotNull(operandTypes, "operandTypes"),
            i -> requiredOperands == null || i + 1 > requiredOperands
        );
      } else if (operandTypes == null && requiredOperands == null) {
        theOperandTypeChecker = operandTypeChecker;
      } else {
        throw new ISE("Cannot have both 'operandTypeChecker' and 'operandTypes' / 'requiredOperands'");
      }

      return new SqlFunction(
          name,
          kind,
          Preconditions.checkNotNull(returnTypeInference, "returnTypeInference"),
          null,
          theOperandTypeChecker,
          functionCategory
      );
    }
  }
}
