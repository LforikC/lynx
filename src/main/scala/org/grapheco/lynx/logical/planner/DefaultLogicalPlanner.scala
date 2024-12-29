package org.grapheco.lynx.logical.planner

import org.grapheco.lynx._
import org.grapheco.lynx.logical.plans.{LogicalCreateIndex, LogicalDropIndex, LogicalPlan}
import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.translators.QueryPartTranslator
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions.{LabelName, Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.ASTNode

class DefaultLogicalPlanner(runnerContext: CypherRunnerContext) extends LogicalPlanner {

  /**
   * 核心翻译方法，将不同类型的 AST 节点翻译为 LogicalPlan
   *
   * @param node 待翻译的 AST 节点
   * @param lpc 上下文对象 LogicalPlannerContext
   * @return 翻译后的 LogicalPlan
   */
  private def translate(node: ASTNode)(implicit lpc: LogicalPlannerContext): LogicalPlan = {
    node match {
      // 处理 Query 节点
      case Query(periodicCommitHint: Option[PeriodicCommitHint], part: QueryPart) =>
        translateQueryPart(part)

      // 处理 CreateUniquePropertyConstraint 节点（暂未实现）
      case CreateUniquePropertyConstraint(Variable(v1), LabelName(l), List(Property(Variable(v2), PropertyKeyName(p)))) =>
        throw new UnsupportedOperationException(s"CreateUniquePropertyConstraint is not supported: ${node.toString}")

      // 处理 CreateIndex 节点
      case CreateIndex(labelName, properties) =>
        translateCreateIndex(labelName, properties)

      // 处理 DropIndex 节点
      case DropIndex(labelName, properties) =>
        translateDropIndex(labelName, properties)

      // 未知节点，抛出更详细的错误
      case _ =>
        throw new logical.UnknownASTNodeException(s"Unsupported ASTNode: ${node.getClass.getName}, content: ${node.toString}")
    }
  }

  /**
   * 翻译 Query 节点
   *
   * @param part Query 的子部分
   * @return 对应的 LogicalPlan
   */
  private def translateQueryPart(part: QueryPart)(implicit lpc: LogicalPlannerContext): LogicalPlan = {
    QueryPartTranslator(part).translate(None)
  }

  /**
   * 翻译 CreateIndex 节点
   *
   * @param labelName 索引的标签名
   * @param properties 索引的属性列表
   * @return 生成的 LogicalCreateIndex 计划
   */
  private def translateCreateIndex(labelName: LabelName, properties: Seq[PropertyKeyName]): LogicalPlan = {
    LogicalCreateIndex(labelName.name, properties.map(_.name))
  }

  /**
   * 翻译 DropIndex 节点
   *
   * @param labelName 索引的标签名
   * @param properties 索引的属性列表
   * @return 生成的 LogicalDropIndex 计划
   */
  private def translateDropIndex(labelName: LabelName, properties: Seq[PropertyKeyName]): LogicalPlan = {
    LogicalDropIndex(labelName.name, properties.map(_.name))
  }

  /**
   * 实现 LogicalPlanner 接口的 plan 方法
   *
   * @param statement 输入的语句 Statement
   * @param plannerContext 翻译上下文
   * @return 对应的 LogicalPlan
   */
  override def plan(statement: Statement, plannerContext: LogicalPlannerContext): LogicalPlan = {
    // 调用 translate 方法处理 AST 节点
    translate(statement)(plannerContext)
  }
}
