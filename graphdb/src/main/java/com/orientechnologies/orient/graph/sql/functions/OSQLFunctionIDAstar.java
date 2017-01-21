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
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * @author Martin Eutinger (m.eutinger a_t googlemail.com) IDA* https://de.wikipedia.org/wiki/IDA*#Algorithmus_.28formal.29
 */
public class OSQLFunctionIDAstar extends OSQLFunctionAstarAbstract {
  public static final String                NAME     = "idastar";
  protected Map<OrientVertex, OrientVertex> cameFrom = new HashMap<OrientVertex, OrientVertex>();
  protected Map<OrientVertex, Double>       gScore   = new HashMap<OrientVertex, Double>();

  public OSQLFunctionIDAstar() {
    super(NAME, 3, 4);
  }

  protected LinkedList<OrientVertex> internalExecute(final OCommandContext iContext, OrientBaseGraph graph) {

    OrientVertex start = paramSourceVertex;
    OrientVertex goal = paramDestinationVertex;
    route.clear();

    OrientVertex solutionVertex = null;
    double limit = getHeuristicCost(start, null, goal);
    double maxLimit = Double.MAX_VALUE;
    while (solutionVertex == null) {
      currentDepth = 0;
      cameFrom.clear();
      gScore.clear();
      gScore.put(start, 0.0); // The cost of going from start to start is zero.
      IDAStarResult result = search(start, null, goal, limit, 0);
      if (paramEmptyIfMaxDepth == true && result.getDepth() >= paramMaxDepth) {
        route.clear();
        return getPath();
      }
      if (result.getSolutionVertex() != null) {
        solutionVertex = result.getSolutionVertex();
      }
      if (result.getLimit() >= maxLimit) {
        return getPath();
      }
      limit = result.getLimit();
    }

    while (solutionVertex != null) {
      route.add(0, solutionVertex);
      solutionVertex = cameFrom.get(solutionVertex);
    }
    return getPath();
  }

  private IDAStarResult search(OrientVertex current, OrientVertex parent, OrientVertex goal, double limit, long depth) {
    currentDepth = depth;
    double currentFScore = gScore.get(current) + getHeuristicCost(current, parent, goal);
    if (currentFScore > limit) {
      return new IDAStarResult(currentFScore, depth);
    }
    if (current.getIdentity().equals(goal.getIdentity()) || depth >= paramMaxDepth) {
      return new IDAStarResult(current);
    }
    double minLimit = Double.MAX_VALUE;
    long maxDepth = 0;
    for (OrientVertex neighbor : getNeighbors(current)) {
      if (isOnPath(neighbor, current)) {
        continue;
      }
      cameFrom.put(neighbor, current);
      gScore.put(neighbor, gScore.get(current) + getDistance(current, current, neighbor));
      IDAStarResult result = search(neighbor, current, goal, limit, depth + 1);
      if (result.getSolutionVertex() != null) {
        return result;
      }
      if (result.getLimit() < minLimit) {
        minLimit = result.getLimit();
      }
      if (result.getDepth() > maxDepth) {
        maxDepth = result.getDepth();
      }
    }
    return new IDAStarResult(minLimit, maxDepth);
  }

  private boolean isOnPath(OrientVertex check, OrientVertex current) {
    while (current != null) {
      if (current.getIdentity().equals(check.getIdentity())) {
        return true;
      }
      current = cameFrom.get(current);
    }
    return false;
  }

  private class IDAStarResult {
    private OrientVertex solutionVertex;
    private double       limit;
    private long         depth;

    public IDAStarResult(OrientVertex solutionVertex) {
      this.solutionVertex = solutionVertex;
    }

    public IDAStarResult(double limit) {
      this.limit = limit;
    }

    public IDAStarResult(double limit, long depth) {
      this.limit = limit;
      this.depth = depth;
    }

    public OrientVertex getSolutionVertex() {
      return solutionVertex;
    }

    public double getLimit() {
      return limit;
    }

    public long getDepth() {
      return depth;
    }
  }

  public String getSyntax() {
    return "idastar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
  }

}