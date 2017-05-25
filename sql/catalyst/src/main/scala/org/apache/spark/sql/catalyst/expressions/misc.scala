/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{BigDecimalUtils, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Print the result of an expression to stderr (used for debugging codegen).
 */
case class PrintToStderr(child: Expression) extends UnaryExpression {

  override def dataType: DataType = child.dataType

  protected override def nullSafeEval(input: Any): Any = input

  private val outputPrefix = s"Result of ${child.simpleString} is "

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val outputPrefixField = ctx.addReferenceObj("outputPrefix", outputPrefix)
    nullSafeCodeGen(ctx, ev, c =>
      s"""
         | System.err.println($outputPrefixField + $c);
         | ${ev.value} = $c;
       """.stripMargin)
  }
}

/**
 * A function throws an exception if 'condition' is not true.
 */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Throws an exception if `expr` is not true.",
  extended = """
    Examples:
      > SELECT _FUNC_(0 < 1);
       NULL
  """)
case class AssertTrue(child: Expression) extends UnaryExpression with ImplicitCastInputTypes {

  override def nullable: Boolean = true

  override def inputTypes: Seq[DataType] = Seq(BooleanType)

  override def dataType: DataType = NullType

  override def prettyName: String = "assert_true"

  private val errMsg = s"'${child.simpleString}' is not true!"

  override def eval(input: InternalRow) : Any = {
    val v = child.eval(input)
    if (v == null || java.lang.Boolean.FALSE.equals(v)) {
      throw new RuntimeException(errMsg)
    } else {
      null
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val eval = child.genCode(ctx)

    // Use unnamed reference that doesn't create a local field here to reduce the number of fields
    // because errMsgField is used only when the value is null or false.
    val errMsgField = ctx.addReferenceMinorObj(errMsg)
    ExprCode(code = s"""${eval.code}
       |if (${eval.isNull} || !${eval.value}) {
       |  throw new RuntimeException($errMsgField);
       |}""".stripMargin, isNull = "true", value = "null")
  }

  override def sql: String = s"assert_true(${child.sql})"
}

/**
 * Returns the current database of the SessionCatalog.
 */
@ExpressionDescription(
  usage = "_FUNC_() - Returns the current database.",
  extended = """
    Examples:
      > SELECT _FUNC_();
       default
  """)
case class CurrentDatabase() extends LeafExpression with Unevaluable {
  override def dataType: DataType = StringType
  override def foldable: Boolean = true
  override def nullable: Boolean = false
  override def prettyName: String = "current_database"
}

/**
 * Returns date truncated to the unit specified by the format or
 * numeric truncated to scale decimal places.
 */
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = """
      _FUNC_(data, fmt) - Returns `data` truncated by the format model `fmt`.
        If `data` is DateType, returns `data` with the time portion of the day truncated to the unit specified by the format model `fmt`.
        If `data` is DoubleType, returns `data` truncated to `fmt` decimal places.
  """,
  extended = """
    Examples:
      > SELECT _FUNC_('2009-02-12', 'MM');
       2009-02-01
      > SELECT _FUNC_('2015-10-27', 'YEAR');
       2015-01-01
      > SELECT _FUNC_(1234567891.1234567891, 4);
       1234567891.1234
      > SELECT _FUNC_(1234567891.1234567891, -4);
       1234560000
             """)
// scalastyle:on line.size.limit
case class Trunc(data: Expression, format: Expression = Literal(0))
  extends BinaryExpression with ImplicitCastInputTypes {

  def this(numeric: Expression) = {
    this(numeric, Literal(0))
  }

  override def left: Expression = data
  override def right: Expression = format

  override def dataType: DataType = data.dataType

  override def inputTypes: Seq[AbstractDataType] =
    Seq(TypeCollection(DoubleType, DateType), TypeCollection(StringType, IntegerType))

  override def nullable: Boolean = true
  override def prettyName: String = "trunc"

  private lazy val truncFormat: Int = dataType match {
    case doubleType: DoubleType =>
      format.eval().asInstanceOf[Int]
    case dateType: DateType =>
      DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
  }

  override def eval(input: InternalRow): Any = {
    val d = data.eval(input)
    val form = format.eval()
    if (null == d || null == form) {
      null
    } else {
      dataType match {
        case doubleType: DoubleType =>
          val scale = if (format.foldable) {
            truncFormat
          } else {
            format.eval().asInstanceOf[Int]
          }
          BigDecimalUtils.trunc(d.asInstanceOf[Double], scale).doubleValue()
        case dateType: DateType =>
          val level = if (format.foldable) {
            truncFormat
          } else {
            DateTimeUtils.parseTruncLevel(format.eval().asInstanceOf[UTF8String])
          }
          if (level == -1) {
            // unknown format
            null
          } else {
            DateTimeUtils.truncDate(d.asInstanceOf[Int], level)
          }
      }
    }

  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {

    dataType match {
      case doubleType: DoubleType =>
        val bdu = BigDecimalUtils.getClass.getName.stripSuffix("$")

        if (format.foldable) {
          val d = data.genCode(ctx)
          ev.copy(code = s"""
            ${d.code}
            boolean ${ev.isNull} = ${d.isNull};
            ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
            if (!${ev.isNull}) {
              ${ev.value} = $bdu.trunc(${d.value}, $truncFormat).doubleValue();
            }""")
        } else {
          nullSafeCodeGen(ctx, ev, (doubleVal, fmt) => {
            s"${ev.value} = $bdu.trunc($doubleVal, $fmt).doubleValue();"
          })
        }
      case dateType: DateType =>
        val dtu = DateTimeUtils.getClass.getName.stripSuffix("$")

        if (format.foldable) {
          if (truncFormat == -1) {
            ev.copy(code = s"""
              boolean ${ev.isNull} = true;
              ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};""")
          } else {
            val d = data.genCode(ctx)
            ev.copy(code = s"""
              ${d.code}
              boolean ${ev.isNull} = ${d.isNull};
              ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
              if (!${ev.isNull}) {
                ${ev.value} = $dtu.truncDate(${d.value}, $truncFormat);
              }""")
          }
        } else {
          nullSafeCodeGen(ctx, ev, (dateVal, fmt) => {
            val form = ctx.freshName("form")
            s"""
              int $form = $dtu.parseTruncLevel($fmt);
              if ($form == -1) {
                ${ev.isNull} = true;
              } else {
                ${ev.value} = $dtu.truncDate($dateVal, $form);
              }
            """
          })
        }
    }

  }
}
