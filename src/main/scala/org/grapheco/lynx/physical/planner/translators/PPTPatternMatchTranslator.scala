package org.grapheco.lynx.physical.planner.translators

import org.grapheco.lynx.logical.plans.LogicalPatternMatch
import org.grapheco.lynx.physical
import org.grapheco.lynx.physical.planner.PPTNodeTranslator
import org.grapheco.lynx.physical.plans.{FromArgument, Expand, NodeScan, RelationshipScan, PhysicalPlan}
import org.grapheco.lynx.physical.PhysicalPlannerContext
import org.opencypher.v9_0.expressions.{NodePattern, RelationshipPattern}

// 优化后的代码逻辑
case class PPTPatternMatchTranslator(patternMatch: LogicalPatternMatch)(implicit val plannerContext: PhysicalPlannerContext) {

  // 抽取逻辑：处理单个链条（关系-节点对）
  private def handleChain(chain: Seq[(RelationshipPattern, NodePattern)], initialPlan: PhysicalPlan)
                         (implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    chain.foldLeft(initialPlan) { case (currentPlan, (relationship, nextNode)) =>
      Expand(relationship, nextNode)(currentPlan, plannerContext)
    }
  }

  // 抽取逻辑：判断是否需要从参数中获取变量的逻辑
  private def getInitialPlan(headNode: NodePattern, argumentHit: Boolean)
                            (implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    if (argumentHit) FromArgument(headNode.variable.get.name)(ppc) // 从已有参数中获取
    else NodeScan(headNode)(ppc) // 否则执行全表扫描
  }

  // 主逻辑：规划模式匹配
  private def planPatternMatch(pm: LogicalPatternMatch)(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    // 判断是否命中参数上下文
    val argumentHit = ppc.argumentContext.contains(patternMatch.headNode.variable.map(_.name).getOrElse(""))
    val LogicalPatternMatch(optional, variableName, headNode: NodePattern, chain: Seq[(RelationshipPattern, NodePattern)]) = pm

    // 获取初始计划（NodeScan 或 FromArgument）
    val initialPlan = getInitialPlan(headNode, argumentHit)

    // 处理链条：递归翻译或逐步扩展
    handleChain(chain, initialPlan)
  }

  // 翻译接口：调用主逻辑
  override def translate(in: Option[PhysicalPlan])(implicit ppc: PhysicalPlannerContext): PhysicalPlan = {
    // 此处的逻辑可以进一步提取到优化器中
    planPatternMatch(patternMatch)(ppc)
  }
}
