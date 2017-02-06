/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Martin Eutinger (m.eutinger a_t googlemail.com) LRTA* based on
 *         https://www.iiia.csic.es/sites/default/files/IIIA-2005-1027.pdf
 */
public class OSQLFunctionLRTAstar extends OSQLFunctionAstarAbstract {
  public static final String          NAME   = "lrtastar";
  protected Map<OrientVertex, Double> hScore = new HashMap<OrientVertex, Double>();
  private double                      startTime;
  private double                      routeLength;

  public OSQLFunctionLRTAstar() {
    super(NAME, 3, 4);
  }

  protected LinkedList<OrientVertex> internalExecute(final OCommandContext iContext, OrientBaseGraph graph) {
    OrientVertex start = paramSourceVertex;
    OrientVertex goal = paramDestinationVertex;

    hScore.clear();
    route.clear();

    List<OrientVertex> bestPreviousRoute = new LinkedList<OrientVertex>();
    double bestPreviousRouteLength = Double.MAX_VALUE;
    startTime = System.nanoTime();
    Map<OrientVertex, Double> old_hScore;

    do {
      old_hScore = new HashMap<OrientVertex, Double>(hScore);
      currentDepth = 0;
      routeLength = 0.0;
      if (!trial(start, goal, graph)) {
        route = removeCycles(bestPreviousRoute);
        break;
      }
      if (bestPreviousRouteLength > routeLength) {
        bestPreviousRouteLength = routeLength;
        bestPreviousRoute = new LinkedList<OrientVertex>(route);
      }
    } while (!hScore.equals(old_hScore));

    // TODO not sure if paramMaxDepth makes sense for lrta
    if (currentDepth >= paramMaxDepth) {
      if (paramEmptyIfMaxDepth == true) {
        route.clear();
        return getPath();
      }
      LinkedList<OrientVertex> path = new LinkedList<OrientVertex>();
      for (int i = 0; i <= paramMaxDepth; i++) {
        path.add(route.get(i));
      }
      return path;
    }

    return getPath();
  }

  private boolean trial(OrientVertex start, OrientVertex goal, OrientBaseGraph graph) {
    route.clear();
    OrientVertex current = start;
    while (!current.getIdentity().equals(goal.getIdentity())) {
      route.add(current);
      OrientVertex successor = getSuccessor(current, goal, graph);
      current = successor;
      if ((System.nanoTime() - startTime) / 1000000 > paramTimeout) {
        return false;
      }
      currentDepth++;
    }
    route.add(current);
    return true;
  }

  private OrientVertex getSuccessor(OrientVertex current, OrientVertex goal, OrientBaseGraph graph) {
    double current_hScore = get_hScore(current, null, goal);

    double minScore = Double.MAX_VALUE;
    double pickDistance = 0.0;
    OrientVertex pick = null;
    for (OrientEdge neighborEdge : getNeighborEdges(current)) {
      OrientVertex neighbor = getNeighbor(current, neighborEdge, graph);
      double neighborDistance = getDistance(neighborEdge);
      double neighborScore = neighborDistance + get_hScore(neighbor, current, goal);
      if (neighborScore < minScore) {
        minScore = neighborScore;
        pick = neighbor;
        pickDistance = neighborDistance;
      }
    }
    routeLength += pickDistance;
    hScore.put(current, Math.max(current_hScore, minScore));
    return pick;
  }

  double get_hScore(final OrientVertex node, OrientVertex parent, final OrientVertex goal) {
    Double node_hScore = hScore.get(node);
    if (node_hScore == null) {
      return getHeuristicCost(node, parent, goal);
    }
    return node_hScore;
  }

  private List<OrientVertex> removeCycles(List<OrientVertex> cycleRoute) {
    List<OrientVertex> uniqueRoute = new LinkedList<OrientVertex>();
    for (int i = 0; i < cycleRoute.size(); i++) {
      OrientVertex current = cycleRoute.get(i);
      uniqueRoute.add(current);
      int indexOfCurrent = cycleRoute.indexOf(current);
      int lastIndexOfCurrent = cycleRoute.lastIndexOf(current);
      if (indexOfCurrent != lastIndexOfCurrent) {
        i = lastIndexOfCurrent;
      }
    }
    return uniqueRoute;
  }

  public String getSyntax() {
    return "lrtastar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
  }
}