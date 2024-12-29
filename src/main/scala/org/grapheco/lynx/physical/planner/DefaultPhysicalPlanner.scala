package org.grapheco.lynx.physical.planner

import org.grapheco.lynx.logical.plans._
import org.grapheco.lynx.physical._
import org.grapheco.lynx.physical.planner.translators._
import org.grapheco.lynx.physical.plans._
import org.grapheco.lynx.runner.CypherRunnerContext
import org.opencypher.v9_0.expressions._

class DefaultPhysicalPlanner(runnerContext: CypherRunnerContext) extends PhysicalPlanner {

  /**
   * 核心计划方法，根据逻辑计划生成物理计划
   *
   * @param logicalPlan 输入的逻辑计划
   * @param plannerContext 上下文对象
   * @return 对应的物理计划
   */
  override def plan(logicalPlan: LogicalPlan)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    implicit val runnerContext: CypherRunnerContext = plannerContext.runnerContext

    logicalPlan match {
      // 各类型逻辑计划的处理分支
      case lp: LogicalProcedureCall   => planProcedureCall(lp)
      case lc: LogicalCreate          => planCreate(lc)
      case lm: LogicalMerge           => planMerge(lm)
      case ld: LogicalDelete          => planDelete(ld)
      case ls: LogicalSelect          => planSelect(ls)
      case lp: LogicalProject         => planProject(lp)
      case la: LogicalAggregation     => planAggregation(la)
      case lf: LogicalFilter          => planFilter(lf)
      case lw: LogicalWith            => planWith(lw)
      case ld: LogicalDistinct        => planDistinct(ld)
      case ll: LogicalLimit           => planLimit(ll)
      case lo: LogicalOrderBy         => planOrderBy(lo)
      case ll: LogicalSkip            => planSkip(ll)
      case lj: LogicalJoin            => planJoin(lj)
      case lc: LogicalCross           => planCross(lc)
      case ap: LogicalAndThen         => planAndThen(ap)
      case pm: LogicalPatternMatch    => planPatternMatch(pm)
      case sp: LogicalShortestPaths   => planShortestPaths(sp)
      case ci: LogicalCreateIndex     => planCreateIndex(ci)
      case di: LogicalDropIndex       => planDropIndex(di)
      case sc: LogicalSetClause       => planSetClause(sc)
      case lr: LogicalRemove          => planRemove(lr)
      case lu: LogicalUnwind          => planUnwind(lu)
      case lu: LogicalUnion           => planUnion(lu)

      // 不支持的逻辑计划，抛出更详细的错误信息
      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported logical plan: ${logicalPlan.getClass.getSimpleName}, content: ${logicalPlan.toString}"
        )
    }
  }

  // 为每个逻辑类型的处理逻辑提取独立方法
  private def planProcedureCall(lp: LogicalProcedureCall)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    ProcedureCall(lp.procedureNamespace, lp.procedureName, lp.declaredArguments)
  }

  private def planCreate(lc: LogicalCreate)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTCreateTranslator(lc.pattern).translate(lc.in.map(plan(_)))
  }

  private def planMerge(lm: LogicalMerge)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTMergeTranslator(lm.pattern, lm.actions).translate(lm.in.map(plan(_)))
  }

  private def planDelete(ld: LogicalDelete)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Delete(ld.expressions, ld.forced)(plan(ld.in), plannerContext)
  }

  private def planSelect(ls: LogicalSelect)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Select(ls.columns)(plan(ls.in), plannerContext)
  }

  private def planProject(lp: LogicalProject)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Project(lp.ri)(plan(lp.in), plannerContext)
  }

  private def planAggregation(la: LogicalAggregation)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Aggregation(la.a, la.g)(plan(la.in), plannerContext)
  }

  private def planFilter(lf: LogicalFilter)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Filter(lf.expr)(plan(lf.in), plannerContext)
  }

  private def planWith(lw: LogicalWith)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    With(lw.ri)(plan(lw.in), plannerContext)
  }

  private def planDistinct(ld: LogicalDistinct)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Distinct()(plan(ld.in), plannerContext)
  }

  private def planLimit(ll: LogicalLimit)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Limit(ll.expr)(plan(ll.in), plannerContext)
  }

  private def planOrderBy(lo: LogicalOrderBy)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    OrderBy(lo.sortItem)(plan(lo.in), plannerContext)
  }

  private def planSkip(ll: LogicalSkip)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Skip(ll.expr)(plan(ll.in), plannerContext)
  }

  private def planJoin(lj: LogicalJoin)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Join(None, lj.isSingleMatch, lj.joinType)(plan(lj.a), plan(lj.b), plannerContext)
  }

  private def planCross(lc: LogicalCross)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Cross()(plan(lc.a), plan(lc.b), plannerContext)
  }

  private def planAndThen(ap: LogicalAndThen)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    val first = plan(ap.first)
    val contextWithArg: PhysicalPlannerContext = plannerContext.withArgumentsContext(first.schema.map(_._1))
    val andThen = plan(ap._then)(contextWithArg)
    Apply()(first, andThen, contextWithArg)
  }

  private def planPatternMatch(pm: LogicalPatternMatch)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTPatternMatchTranslator(pm)(plannerContext).translate(None)
  }

  private def planShortestPaths(sp: LogicalShortestPaths)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    LPTShortestPathTranslator(sp)(plannerContext).translate(None)
  }

  private def planCreateIndex(ci: LogicalCreateIndex)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    CreateIndex(ci.labelName, ci.properties)(plannerContext)
  }

  private def planDropIndex(di: LogicalDropIndex)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    DropIndex(di.labelName, di.properties)(plannerContext)
  }

  private def planSetClause(sc: LogicalSetClause)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTSetClauseTranslator(sc.items).translate(sc.in.map(plan(_)))(plannerContext)
  }

  private def planRemove(lr: LogicalRemove)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTRemoveTranslator(lr.items).translate(lr.in.map(plan(_)))(plannerContext)
  }

  private def planUnwind(lu: LogicalUnwind)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    PPTUnwindTranslator(lu.expression, lu.variable).translate(lu.in.map(plan(_)))(plannerContext)
  }

  private def planUnion(lu: LogicalUnion)(implicit plannerContext: PhysicalPlannerContext): PhysicalPlan = {
    Union(lu.distinct)(plan(lu.a), plan(lu.b), plannerContext)
  }
}
