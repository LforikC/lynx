package org.grapheco.lynx.logical.planner.translators

import org.grapheco.lynx.logical.LogicalPlannerContext
import org.grapheco.lynx.logical.planner.LogicalTranslator
import org.grapheco.lynx.logical.plans._
import org.opencypher.v9_0.ast.{ReturnItems, Where, With}

/**
 * @ClassName WithTranslator
 * @Description 将 WITH 语句从 AST 转换为 LogicalPlan
 */
case class WithTranslator(w: With) extends LogicalTranslator {

  /**
   * 核心翻译函数，将 AST 的 With 转换为 LogicalPlan
   * 
   * @param in 上游 LogicalPlan
   * @param plannerContext 翻译上下文
   * @return 生成的 LogicalPlan
   */
  def translate(in: Option[LogicalPlan])(implicit plannerContext: LogicalPlannerContext): LogicalPlan = {
    (w, in) match {
      // Case 1: 没有上游逻辑计划，直接创建单位 LogicalPlan
      case (With(distinct, ReturnItems(includeExisting, items), orderBy, skip, limit: Option[LogicalLimit], where), None) =>
        LogicalCreateUnit(items) // 直接生成 LogicalCreateUnit

      // Case 2: 存在上游逻辑计划，对 With 的各个部分逐步翻译处理
      case (With(distinct, ri: ReturnItems, orderBy, skip: Option[LogicalSkip], limit: Option[LogicalLimit], where: Option[Where]), Some(sin)) =>
        LogicalWith(ri)(processPipedTranslators(distinct, ri, where, orderBy, skip, limit).translate(in))
    }
  }

  /**
   * 处理 PipedTranslators 的核心逻辑，解耦各部分逻辑的翻译
   *
   * @param distinct 是否包含 DISTINCT
   * @param ri 返回项 ReturnItems
   * @param where WHERE 子句
   * @param orderBy ORDER BY 子句
   * @param skip SKIP 子句
   * @param limit LIMIT 子句
   * @return 生成的 PipedTranslators
   */
  private def processPipedTranslators(
      distinct: Boolean,
      ri: ReturnItems,
      where: Option[Where],
      orderBy: Option[LogicalOrderBy],
      skip: Option[LogicalSkip],
      limit: Option[LogicalLimit]
  )(implicit plannerContext: LogicalPlannerContext): PipedTranslators = {
    // 分别翻译各个部分并组合成管道
    PipedTranslators(
      Seq(
        // 处理返回项
        ReturnItemsTranslator(ri),
        // WHERE 子句处理
        WhereTranslator(where),
        // ORDER BY 子句处理
        OrderByTranslator(orderBy),
        // SKIP 子句处理
        SkipTranslator(skip),
        // LIMIT 子句处理
        LimitTranslator(limit),
        // DISTINCT 处理
        DistinctTranslator(distinct)
      )
    )
  }
}
