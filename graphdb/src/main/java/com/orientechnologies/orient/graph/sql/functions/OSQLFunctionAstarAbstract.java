/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;

/**
 * An abstract superclass for A*-like OSQLFunctions.
 * 
 * @author Saeed Tabrizi (saeed a_t nowcando.com)
 */
public abstract class OSQLFunctionAstarAbstract extends OSQLFunctionHeuristicPathFinderAbstract {

  private String paramWeightFieldName = "weight";
  protected long currentDepth         = 0;

  public OSQLFunctionAstarAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  public LinkedList<OrientVertex> execute(final Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult,
      final Object[] iParams, final OCommandContext iContext) {
    context = iContext;
    final OSQLFunctionAstarAbstract context = this;
    return OGraphCommandExecutorSQLFactory
        .runWithAnyGraph(new OGraphCommandExecutorSQLFactory.GraphCallBack<LinkedList<OrientVertex>>() {
          @Override
          public LinkedList<OrientVertex> call(final OrientBaseGraph graph) {

            final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

            Object source = iParams[0];
            if (OMultiValue.isMultiValue(source)) {
              if (OMultiValue.getSize(source) > 1)
                throw new IllegalArgumentException("Only one sourceVertex is allowed");
              source = OMultiValue.getFirstValue(source);
            }
            paramSourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));

            Object dest = iParams[1];
            if (OMultiValue.isMultiValue(dest)) {
              if (OMultiValue.getSize(dest) > 1)
                throw new IllegalArgumentException("Only one destinationVertex is allowed");
              dest = OMultiValue.getFirstValue(dest);
            }
            paramDestinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));

            paramWeightFieldName = OIOUtils.getStringContent(iParams[2]);

            if (iParams.length > 3) {
              bindAdditionalParams(iParams[3], context);
            }
            iContext.setVariable("getNeighbors", 0);
            return internalExecute(iContext, graph);
          }
        });
  }

  protected abstract LinkedList<OrientVertex> internalExecute(final OCommandContext iContext, OrientBaseGraph graph);

  protected OrientVertex getNeighbor(OrientVertex current, OrientEdge neighborEdge, OrientBaseGraph graph) {
    if (neighborEdge.getOutVertex().equals(current)) {
      return toVertex(neighborEdge.getInVertex(), graph);
    }
    return toVertex(neighborEdge.getOutVertex(), graph);
  }

  private OrientVertex toVertex(OIdentifiable outVertex, OrientBaseGraph graph) {
    if (outVertex == null) {
      return null;
    }
    if (outVertex instanceof OrientVertex) {
      return (OrientVertex) outVertex;
    }
    return graph.getVertex(outVertex);
  }

  protected Set<OrientEdge> getNeighborEdges(final OrientVertex node) {
    context.incrementVariable("getNeighbors");

    final Set<OrientEdge> neighbors = new HashSet<OrientEdge>();
    if (node != null) {
      for (Edge v : node.getEdges(paramDirection, paramEdgeTypeNames)) {
        final OrientEdge ov = (OrientEdge) v;
        if (ov != null)
          neighbors.add(ov);
      }
    }
    return neighbors;
  }

  private void bindAdditionalParams(Object additionalParams, OSQLFunctionAstarAbstract ctx) {
    if (additionalParams == null) {
      return;
    }
    Map<String, Object> mapParams = null;
    if (additionalParams instanceof Map) {
      mapParams = (Map) additionalParams;
    } else if (additionalParams instanceof OIdentifiable) {
      mapParams = ((ODocument) ((OIdentifiable) additionalParams).getRecord()).toMap();
    }
    if (mapParams != null) {
      ctx.paramEdgeTypeNames = stringArray(mapParams.get(OSQLFunctionAstarAbstract.PARAM_EDGE_TYPE_NAMES));
      ctx.paramVertexAxisNames = stringArray(mapParams.get(OSQLFunctionAstarAbstract.PARAM_VERTEX_AXIS_NAMES));
      if (mapParams.get(OSQLFunctionAstarAbstract.PARAM_DIRECTION) != null) {
        if (mapParams.get(OSQLFunctionAstarAbstract.PARAM_DIRECTION) instanceof String) {
          ctx.paramDirection = Direction
              .valueOf(stringOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_DIRECTION), "OUT").toUpperCase());
        } else {
          ctx.paramDirection = (Direction) mapParams.get(OSQLFunctionAstarAbstract.PARAM_DIRECTION);
        }
      }

      ctx.paramParallel = booleanOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_PARALLEL), false);
      ctx.paramMaxDepth = longOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_MAX_DEPTH), ctx.paramMaxDepth);
      ctx.paramEmptyIfMaxDepth = booleanOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_EMPTY_IF_MAX_DEPTH),
          ctx.paramEmptyIfMaxDepth);
      ctx.paramTieBreaker = booleanOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_TIE_BREAKER), ctx.paramTieBreaker);
      ctx.paramDFactor = doubleOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_D_FACTOR), ctx.paramDFactor);
      if (mapParams.get(OSQLFunctionAstarAbstract.PARAM_HEURISTIC_FORMULA) != null) {
        if (mapParams.get(OSQLFunctionAstarAbstract.PARAM_HEURISTIC_FORMULA) instanceof String) {
          ctx.paramHeuristicFormula = HeuristicFormula
              .valueOf(stringOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_HEURISTIC_FORMULA), "MANHATTAN").toUpperCase());
        } else {
          ctx.paramHeuristicFormula = (HeuristicFormula) mapParams.get(OSQLFunctionAstarAbstract.PARAM_HEURISTIC_FORMULA);
        }
      }
      ctx.paramHaversineRadius = doubleOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_HAVERSINE_RADIUS), ctx.paramHaversineRadius);
      ctx.paramTimeout = doubleOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_TIMEOUT), ctx.paramTimeout);
      ctx.paramCustomHeuristicFormula = stringOrDefault(mapParams.get(OSQLFunctionAstarAbstract.PARAM_CUSTOM_HEURISTIC_FORMULA),
          "");
    }
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  @Override
  protected double getDistance(final OrientVertex node, final OrientVertex parent, final OrientVertex target) {
    final Iterator<Edge> edges = node.getEdges(target, paramDirection).iterator();
    if (edges.hasNext()) {
      final Edge e = edges.next();
      if (e != null) {
        final Object fieldValue = e.getProperty(paramWeightFieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Float)
            return (Float) fieldValue;
          else if (fieldValue instanceof Number)
            return ((Number) fieldValue).doubleValue();
      }
    }
    return MIN;
  }

  protected double getDistance(final OrientEdge edge) {
    if (edge != null) {
      final Object fieldValue = edge.getProperty(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float)
          return (Float) fieldValue;
        else if (fieldValue instanceof Number)
          return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  protected double getHeuristicCost(final OrientVertex node, OrientVertex parent, final OrientVertex target) {
    double hresult = 0.0;

    if (paramVertexAxisNames.length == 0) {
      return hresult;
    } else if (paramVertexAxisNames.length == 1) {
      double n = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]), 0.0);
      double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]), 0.0);
      hresult = getSimpleHeuristicCost(n, g, paramDFactor);
    } else if (paramVertexAxisNames.length == 2) {
      if (parent == null)
        parent = node;
      double sx = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[0]), 0);
      double sy = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[1]), 0);
      double nx = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]), 0);
      double ny = doubleOrDefault(node.getProperty(paramVertexAxisNames[1]), 0);
      double px = doubleOrDefault(parent.getProperty(paramVertexAxisNames[0]), 0);
      double py = doubleOrDefault(parent.getProperty(paramVertexAxisNames[1]), 0);
      double gx = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]), 0);
      double gy = doubleOrDefault(target.getProperty(paramVertexAxisNames[1]), 0);

      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(nx, ny, gx, gy, paramDFactor);
        break;
      case HAVERSINE:
        hresult = getHaversineHeuristicCost(nx, ny, gx, gy, paramDFactor, paramHaversineRadius);
        break;
      case CUSTOM:
        hresult = getCustomHeuristicCost(paramCustomHeuristicFormula, paramVertexAxisNames, paramSourceVertex,
            paramDestinationVertex, node, parent, currentDepth, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
      }

    } else {
      Map<String, Double> sList = new HashMap<String, Double>();
      Map<String, Double> cList = new HashMap<String, Double>();
      Map<String, Double> pList = new HashMap<String, Double>();
      Map<String, Double> gList = new HashMap<String, Double>();
      parent = parent == null ? node : parent;
      for (int i = 0; i < paramVertexAxisNames.length; i++) {
        Double s = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[i]), 0);
        Double c = doubleOrDefault(node.getProperty(paramVertexAxisNames[i]), 0);
        Double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[i]), 0);
        Double p = doubleOrDefault(parent.getProperty(paramVertexAxisNames[i]), 0);
        if (s != null)
          sList.put(paramVertexAxisNames[i], s);
        if (c != null)
          cList.put(paramVertexAxisNames[i], s);
        if (g != null)
          gList.put(paramVertexAxisNames[i], g);
        if (p != null)
          pList.put(paramVertexAxisNames[i], p);
      }
      switch (paramHeuristicFormula) {
      case MANHATTAN:
        hresult = getManhattanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case MAXAXIS:
        hresult = getMaxAxisHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case DIAGONAL:
        hresult = getDiagonalHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEAN:
        hresult = getEuclideanHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case EUCLIDEANNOSQR:
        hresult = getEuclideanNoSQRHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
        break;
      case CUSTOM:
        hresult = getCustomHeuristicCost(paramCustomHeuristicFormula, paramVertexAxisNames, paramSourceVertex,
            paramDestinationVertex, node, parent, currentDepth, paramDFactor);
        break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(paramVertexAxisNames, sList, cList, pList, gList, currentDepth, hresult);
      }

    }

    return hresult;

  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }

}