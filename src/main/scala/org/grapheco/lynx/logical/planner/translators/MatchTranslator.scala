package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.dataframe.{InnerJoin, LeftJoin, OuterJoin}
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans._
import org.grapheco.lynx.logical.{LogicalPlannerContext, plans}
import org.opencypher.v9_0.ast._
import org.opencypher.v9_0.expressions._

import scala.annotation.tailrec
import scala.collection.immutable.Seq // 使用不可变集合，避免线程安全问题

/**
 * @ClassName MatchTranslator
 * @Description 将 MATCH 语句从 AST 转换为 LogicalPlan
 */
case class MatchTranslator(m: Match) extends LogicalTranslator {

  /**
   * 将 MATCH 语句翻译为 LogicalPlan
   *
   * @param in           上游 LogicalPlan
   * @param plannerContext 上下文，提供翻译过程中的额外信息
   * @return             翻译后的 LogicalPlan
   */
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    val Match(optional, Pattern(patternParts), _, where) = m

    // Step 1: 翻译每个 PatternPart（如路径、关系链等）
    val translatedParts = patternParts.map(part => matchPatternPart(optional, part, variableName = None))

    // Step 2: 提取 LogicalPatternMatch 部分并合并相邻的匹配模式
    val patternMatches = translatedParts.collect { case p: LogicalPatternMatch => p }
    val combinedMatches = combinePatternMatch(patternMatches)

    // Step 3: 将非 LogicalPatternMatch 的部分追加到合并结果中
    val combinedPlans = combinedMatches ++ translatedParts.filterNot(_.isInstanceOf[LogicalPatternMatch])

    // Step 4: 构建匹配后的逻辑计划（对多个部分进行 Join）
    val matchedPlan = combinedPlans.drop(1).foldLeft(combinedPlans.head) { (a, b) =>
      plans.LogicalJoin(isOuterJoin = true, joinType = OuterJoin)(a, b) // 默认使用 OuterJoin
    }

    // Step 5: 应用 WHERE 子句进行过滤
    val filteredPlan = WhereTranslator(where).translate(Some(matchedPlan))

    // Step 6: 根据是否存在输入 LogicalPlan 合并上下游逻辑计划
    in match {
      case None                          => filteredPlan // 没有上游计划，直接返回
      case Some(w: LogicalWith)          => LogicalAndThen()(w, filteredPlan)
      case Some(u: LogicalUnwind)        => LogicalAndThen()(u, filteredPlan)
      case Some(a: LogicalAndThen)       => LogicalAndThen()(a, filteredPlan)
      case Some(left) if optional        => plans.LogicalJoin(isOuterJoin = false, joinType = LeftJoin)(left, filteredPlan) // 可选匹配
      case Some(left)                    => plans.LogicalJoin(isOuterJoin = false, joinType = InnerJoin)(left, filteredPlan) // 必选匹配
    }
  }

  /**
   * 合并相邻的 LogicalPatternMatch 模式
   *
   * @param patterns 需要合并的 LogicalPatternMatch 列表
   * @return 合并后的 LogicalPatternMatch 列表
   */
  private def combinePatternMatch(patterns: Seq[LogicalPatternMatch]): Seq[LogicalPatternMatch] = {
    // 使用不可变集合 foldLeft 合并相邻 LogicalPatternMatch
    patterns.foldLeft(Seq.empty[LogicalPatternMatch]) { (combined, current) =>
      combined.lastOption match {
        // 如果当前 LogicalPatternMatch 的头节点与前一个 LogicalPatternMatch 的尾节点相连，则合并
        case Some(LogicalPatternMatch(origin, node, head, chain))
            if chain.lastOption.map(_._2).getOrElse(head).variable == current.headNode.variable =>
          combined.dropRight(1) :+ LogicalPatternMatch(origin, node, head, chain ++ current.chain)
        // 否则直接追加到结果中
        case _ => combined :+ current
      }
    }
  }

  /**
   * 将单个 PatternPart 翻译为 LogicalPlan
   *
   * @param optional     是否为可选匹配
   * @param patternPart  匹配模式的部分
   * @param variableName 可选的变量名
   * @return LogicalPlan
   */
  @tailrec
  private def matchPatternPart(optional: Boolean, patternPart: PatternPart, variableName: Option[String]): LogicalPlan = {
    patternPart match {
      case EveryPath(element: PatternElement) =>
        // 匹配路径模式
        val matchedPattern = matchPattern(element)
        LogicalPatternMatch(optional, variableName, matchedPattern._1, matchedPattern._2)
      case ShortestPaths(element, single) =>
        // 匹配最短路径模式
        val matchedPattern = matchPattern(element)
        LogicalShortestPaths(matchedPattern._1, matchedPattern._2, single, variableName.getOrElse("UNNAMED"))
      case NamedPatternPart(variable: Variable, inner: AnonymousPatternPart) =>
        // 递归处理命名的模式部分
        matchPatternPart(optional, inner, Some(variable.name))
    }
  }

  /**
   * 将 PatternElement 匹配为节点和关系的组合
   *
   * @param element 模式元素（节点或关系链）
   * @return 节点模式和关系链
   */
  private def matchPattern(element: PatternElement): (NodePattern, Seq[(RelationshipPattern, NodePattern)]) = {
    element match {
      // 匹配单个节点
      case np: NodePattern => (np, Seq.empty)

      // 匹配简单关系链
      case RelationshipChain(leftNode: NodePattern, relationship: RelationshipPattern, rightNode: NodePattern) =>
        (leftNode, Seq(relationship -> rightNode))

      // 匹配复杂关系链（嵌套关系链）
      case RelationshipChain(leftChain: RelationshipChain, relationship: RelationshipPattern, rightNode: NodePattern) =>
        val leftMatch = matchPattern(leftChain)
        (leftMatch._1, leftMatch._2 :+ (relationship -> rightNode))
    }
  }
}
