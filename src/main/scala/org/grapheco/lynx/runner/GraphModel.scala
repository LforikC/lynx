package org.grapheco.lynx.runner

import org.grapheco.lynx.physical.{NodeInput, RelationshipInput}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxInteger
import org.grapheco.lynx.types.structural._
import org.opencypher.v9_0.expressions.SemanticDirection
import org.opencypher.v9_0.expressions.SemanticDirection.{BOTH, INCOMING, OUTGOING}

import scala.collection.mutable

trait GraphModel {

  /**
   * A Statistics object needs to be returned.
   * In the default implementation, those number is obtained through traversal and filtering.
   * You can override the default implementation.
   *
   * @return The Statistics object
   */
  def statistics: Statistics = new Statistics {
    override def numNode: Long = nodes().length

    override def numNodeByLabel(labelName: LynxNodeLabel): Long = nodes(NodeFilter(Seq(labelName), Map.empty)).length

    override def numNodeByProperty(labelName: LynxNodeLabel, propertyName: LynxPropertyKey, value: LynxValue): Long =
      nodes(NodeFilter(Seq(labelName), Map(propertyName -> value))).length

    override def numRelationship: Long = relationships().length

    override def numRelationshipByType(typeName: LynxRelationshipType): Long =
      relationships(RelationshipFilter(Seq(typeName), Map.empty)).length
  }

  /**
   * An IndexManager object needs to be returned.
   * In the default implementation, the returned indexes is empty,
   * and the addition and deletion of any index will throw an exception.
   * You need override the default implementation.
   *
   * @return The IndexManager object
   */
/*  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index creation")

    override def dropIndex(index: Index): Unit = throw NoIndexManagerException(s"There is no index manager to handle index dropping")

    override def indexes: Array[Index] = Array.empty
  }*/
//定义 indexManager, createIndex 和 dropIndex 抛出的 NoIndexManagerException 异常信息更加详细，添加了要操作的索引信息，方便调试和错误追踪。
  def indexManager: IndexManager = new IndexManager {
    override def createIndex(index: Index): Unit =
      throw new NoIndexManagerException(s"No index manager available to create index $index")

    override def dropIndex(index: Index): Unit =
      throw new NoIndexManagerException(s"No index manager available to drop index $index")

    override def indexes: Array[Index] = Array.empty
  }

  /**
   * An WriteTask object needs to be returned.
   * There is no default implementation, you must override it.
   *
   * @return The WriteTask object
   */
  def write: WriteTask

  /**
   * Find Node By ID.
   *
   * @return An Option of node.
   */
  def nodeAt(id: LynxId): Option[LynxNode]

  /**
   * All nodes.
   *
   * @return An Iterator of all nodes.
   */
  def nodes(): Iterator[LynxNode]


  def getIdPropKey = LynxPropertyKey("_lynx_sys_id")

  case class NodeId(value: Long) extends LynxId {
    override def toLynxInteger: LynxInteger = LynxInteger(value)

    override def toString: String = value.toString
  }

  /**
   * All nodes with a filter.
   *
   * @param nodeFilter The filter
   * @return An Iterator of all nodes after filter.
   */
  def nodes(nodeFilter: NodeFilter): Iterator[LynxNode] = {
    nodeFilter.properties.get(getIdPropKey) match {
      case None => nodes().filter(nodeFilter.matches(_))
      case Some(LynxInteger(nodeId: Long)) => nodeAt(NodeId(nodeId)).iterator.filter(nodeFilter.matches(_, getIdPropKey))
    }
  }

  /**
   * Return all relationships as PathTriple.
   *
   * @return An Iterator of PathTriple
   */
  def relationships(): Iterator[PathTriple]

  /**
   * Return all relationships as PathTriple with a filter.
   *
   * @param relationshipFilter The filter
   * @return An Iterator of PathTriple after filter
   */
  def relationships(relationshipFilter: RelationshipFilter): Iterator[PathTriple] = relationships().filter(f => relationshipFilter.matches(f.storedRelation))


  def createElements[T](nodesInput: Seq[(String, NodeInput)],
                        relationshipsInput: Seq[(String, RelationshipInput)],
                        onCreated: (Seq[(String, LynxNode)], Seq[(String, LynxRelationship)]) => T): T =
    this.write.createElements(nodesInput, relationshipsInput, onCreated)

  def deleteRelations(ids: Iterator[LynxId]): Unit = this.write.deleteRelations(ids)

  def deleteNodes(ids: Seq[LynxId]): Unit = this.write.deleteNodes(ids)

  def setNodesProperties(nodeIds: Iterator[LynxId], data: Array[(String, LynxValue)], cleanExistProperties: Boolean = false): Iterator[Option[LynxNode]] =
    this.write.setNodesProperties(nodeIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)), cleanExistProperties)

  def setNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.setNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def setRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[(String, LynxValue)], cleanExistProperties: Boolean = false): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsProperties(relationshipIds, data.map(kv => (LynxPropertyKey(kv._1), kv._2)), cleanExistProperties)

  def setRelationshipsType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.setRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  def removeNodesProperties(nodeIds: Iterator[LynxId], data: Array[String]): Iterator[Option[LynxNode]] =
    this.write.removeNodesProperties(nodeIds, data.map(LynxPropertyKey))

  def removeNodesLabels(nodeIds: Iterator[LynxId], labels: Array[String]): Iterator[Option[LynxNode]] =
    this.write.removeNodesLabels(nodeIds, labels.map(LynxNodeLabel))

  def removeRelationshipsProperties(relationshipIds: Iterator[LynxId], data: Array[String]): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsProperties(relationshipIds, data.map(LynxPropertyKey))

  def removeRelationshipType(relationshipIds: Iterator[LynxId], theType: String): Iterator[Option[LynxRelationship]] =
    this.write.removeRelationshipsType(relationshipIds, LynxRelationshipType(theType))

  /**
   * Delete nodes in a safe way, and handle nodes with relationships in a special way.
   *
   * @param nodesIDs The ids of nodes to deleted
   * @param forced   When some nodes have relationships,
   *                 if it is true, delete any related relationships,
   *                 otherwise throw an exception
   */
  def deleteNodesSafely(nodesIDs: Iterator[LynxId], forced: Boolean): Unit = {
    val ids = nodesIDs.toSet
    val affectedRelationships = relationships().map(_.storedRelation)
      .filter(rel => ids.contains(rel.startNodeId) || ids.contains(rel.endNodeId))
    if (affectedRelationships.nonEmpty) {
      if (forced)
        this.write.deleteRelations(affectedRelationships.map(_.id))
      else
        throw ConstrainViolatedException(s"deleting referred nodes")
    }
    this.write.deleteNodes(ids.toSeq)
  }

  def commit(): Boolean = this.write.commit

  def singleShortestPath(startNodeId: LynxId,
                         endNodeId: LynxId,
                         relationship: RelationshipFilter,
                         direction: SemanticDirection,
                         lowerLimit: Int = 0,
                         upperLimit: Int = 64
                        ): LynxPath = {
    val paths: mutable.Seq[LynxPath] = allPaths(startNodeId, endNodeId, relationship, direction, lowerLimit, upperLimit)
    if (paths.isEmpty) LynxPath.EMPTY else paths.head
  }


  def allShortestPaths(startNodeId: LynxId,
                       endNodeId: LynxId,
                       relationship: RelationshipFilter,
                       direction: SemanticDirection,
                       lowerLimit: Int = 0,
                       upperLimit: Int = 64
                      ): mutable.Seq[LynxPath] = {
    val paths: mutable.Seq[LynxPath] = allPaths(startNodeId, endNodeId, relationship, direction, lowerLimit, upperLimit)
    if (paths.isEmpty) mutable.Seq() else paths.filter(path => path.nodeIds.length == paths.map(path => path.nodeIds.length).min)
  }

  /**
   * Get the paths that meets the conditions
   *
   * @param startNodeFilter    Filter condition of starting node
   * @param relationshipFilter Filter conditions for relationships
   * @param endNodeFilter      Filter condition of ending node
   * @param direction          Direction of relationships, INCOMING, OUTGOING or BOTH
   * @param upperLimit         Upper limit of relationship length
   * @param lowerLimit         Lower limit of relationship length
   * @return The paths
   */
  def paths(startNodeFilter: NodeFilter,
            relationshipFilter: RelationshipFilter,
            endNodeFilter: NodeFilter,
            direction: SemanticDirection,
            upperLimit: Int,
            lowerLimit: Int): Iterator[LynxPath] = {
    val originStations = nodes(startNodeFilter)
    originStations.flatMap{ originStation =>
      val firstStop = expandNonStop(originStation, relationshipFilter, direction, lowerLimit)
      val leftSteps = Math.min(upperLimit, 100) - lowerLimit // TODO set a super upperLimit
      firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
    }.filter(_.endNode.forall(endNodeFilter.matches(_)))
  }// path 新增节点的不能是已有的节点!!!

  def varExpand(start: LynxNode,
                relationshipFilter: RelationshipFilter,
                direction: SemanticDirection,
                upperLimit: Int,
                lowerLimit: Int): Iterator[LynxPath] = {
    val firstStop = expandNonStop(start, relationshipFilter, direction, lowerLimit)
    val leftSteps = Math.min(upperLimit, 100) - lowerLimit // TODO set a super upperLimit
    firstStop.flatMap(p => extendPath(p, relationshipFilter, direction, leftSteps))
  }

  /**
   * Take a node as the starting or ending node and expand in a certain direction.
   *
   * @param nodeId    The id of this node
   * @param direction The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion
   */
  def expand(nodeId: LynxId, direction: SemanticDirection): Iterator[PathTriple] = {
    (direction match {
      case BOTH => relationships().flatMap(item =>
        Seq(item, item.revert))
      case INCOMING => relationships().map(_.revert)
      case OUTGOING => relationships()
    }).filter(_.startNode.id == nodeId)
  }

  /**
   * Take a node as the starting or ending node and expand in a certain direction with some filter.
   *
   * @param nodeId             The id of this node
   * @param relationshipFilter conditions for relationships
   * @param endNodeFilter      Filter condition of ending node
   * @param direction          The direction of expansion, INCOMING, OUTGOING or BOTH
   * @return Triples after expansion and filter
   */
  def expand(nodeId: LynxId,
             relationshipFilter: RelationshipFilter,
             endNodeFilter: NodeFilter,
             direction: SemanticDirection): Iterator[PathTriple] =
    expand(nodeId, direction).filter { pathTriple =>
      relationshipFilter.matches(pathTriple.storedRelation) && endNodeFilter.matches(pathTriple.endNode)
    }

  def expand(id: LynxId, filter: RelationshipFilter, direction: SemanticDirection): Iterator[PathTriple] =
    expand(id, direction).filter(t => filter.matches(t.storedRelation))

  /*
      Zero length paths

      Using variable length paths that have the lower bound zero means that two variables can point to
      the same node. If the path length between two nodes is zero, they are by definition the same node.
      Note that when matching zero length paths the result may contain a match even when matching on a
      relationship type not in use.
   */
  def expandNonStop(start: LynxNode, relationshipFilter: RelationshipFilter, direction: SemanticDirection, steps: Int): Iterator[LynxPath] = {
    if (steps < 0) return Iterator(LynxPath.EMPTY)
    if (steps == 0) return Iterator(LynxPath.startPoint(start))
//    expand(start.id, relationshipFilter, direction).flatMap{ triple =>
//      expandNonStop(triple.endNode, relationshipFilter, direction, steps - 1).map{_.connectLeft(triple.toLynxPath)}
//    }
    // TODO check cycle
    expand(start.id, relationshipFilter, direction)
    .flatMap { triple =>
      expandNonStop(triple.endNode, relationshipFilter, direction, steps - 1)
        .filterNot(_.nodeIds.contains(triple.startNode.id))
        .map {
        _.connectLeft(triple.toLynxPath)
      }
    }
  }

  def extendPath(path: LynxPath, relationshipFilter: RelationshipFilter, direction: SemanticDirection, steps: Int): Iterator[LynxPath] = {
    if (path.isEmpty || steps <= 0 ) return Iterator(path)
    Iterator(path) ++
      expand(path.endNode.get.id, relationshipFilter, direction)
        .filterNot(tri => path.nodeIds.contains(tri.endNode.id))
        .map(_.toLynxPath)
        .map(_.connectLeft(path)).flatMap(p => extendPath(p, relationshipFilter, direction, steps - 1))
  }
  /**
   * GraphHelper
   */
  val _helper: GraphModelHelper = GraphModelHelper(this)

  // TODO: @LIUYINGDI
  class Hit(var node: LynxNode, var path: LynxPath, var set: mutable.Set[LynxNode])

  object Hit {
    def init(node: LynxNode): Hit = {
      new Hit(node, LynxPath.startPoint(node), mutable.Set(node))
    }

    def extend(node: LynxNode, hit: Hit): Hit = {
      new Hit(node, hit.path.append(node), hit.set + node)
    }
  }


  def allPaths(startNodeId: LynxId,
               endNodeId: LynxId,
               relationship: RelationshipFilter,
               direction: SemanticDirection,
               lowerLimit: Int = 0,
               upperLimit: Int = 64
              ): mutable.Seq[LynxPath] = {
    if (lowerLimit > upperLimit) {
      throw new IllegalArgumentException("IllegalArgumentException: `lowerLimit` cannot be greater than `upperLimit`.")
    }
    val startNode: LynxNode = nodeAt(startNodeId).get
    val endNode: LynxNode = nodeAt(endNodeId).get
    var data: mutable.Seq[mutable.Seq[Hit]] = mutable.Seq(mutable.Seq(Hit.init(startNode)))
    var continueExtend = true
    var nextStep = 1
    while (data.length <= upperLimit && continueExtend) {
      var nextHits: mutable.Seq[Hit] = mutable.Seq.empty
      for (hit: Hit <- data(nextStep - 1)) {
        if (hit.node != endNode) {
          for (nextNode: PathTriple <- expand(hit.node.id, relationship, direction)) {
            if (!hit.set.contains(nextNode.endNode)) {
              nextHits = nextHits :+ Hit.extend(nextNode.endNode, hit)
            }
          }
        }
      }
      if (nextHits.isEmpty) {
        continueExtend = false
      } else {
        data = data :+ nextHits
      }
      nextStep += 1
    }
    var paths: mutable.Seq[LynxPath] = mutable.Seq.empty
    if (lowerLimit < data.length) {
      for (index <- lowerLimit to math.min(upperLimit, data.length - 1)) {
        for (hit <- data(index)) {
          if (hit.node == endNode) {
            paths = paths :+ hit.path
          }
        }
      }
    }
    // for (path <- paths) println(path.nodeIds)
    paths
  }

}
