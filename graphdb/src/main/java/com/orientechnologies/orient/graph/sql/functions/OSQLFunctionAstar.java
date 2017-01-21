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

import com.orientechnologies.orient.core.command.OCommandContext;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;

/**
 * A*'s algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph with husrestic function.
 * <p>
 * The first parameter is source record. The second parameter is destination record. The third parameter is a name of property that
 * represents 'weight' and fourth represnts the map of options.
 * <p>
 * If property is not defined in edge or is null, distance between vertexes are 0 .
 *
 * @author Saeed Tabrizi (saeed a_t  nowcando.com)
 */
public class OSQLFunctionAstar extends OSQLFunctionAstarAbstract {
  public static final String NAME = "astar";

  protected Set<OrientVertex>               closedSet            = new HashSet<OrientVertex>();
  protected Map<OrientVertex, OrientVertex> cameFrom             = new HashMap<OrientVertex, OrientVertex>();

  protected Map<OrientVertex, Double>   gScore = new HashMap<OrientVertex, Double>();
  protected Map<OrientVertex, Double>   fScore = new HashMap<OrientVertex, Double>();
  protected PriorityQueue<OrientVertex> open   = new PriorityQueue<OrientVertex>(1, new Comparator<OrientVertex>() {

    public int compare(OrientVertex nodeA, OrientVertex nodeB) {
      return Double.compare(fScore.get(nodeA), fScore.get(nodeB));
    }
  });

  public OSQLFunctionAstar() {
    super(NAME, 3, 4);
  }

  protected LinkedList<OrientVertex> internalExecute(final OCommandContext iContext, OrientBaseGraph graph) {

    OrientVertex start = paramSourceVertex;
    OrientVertex goal = paramDestinationVertex;

    open.add(start);

    // The cost of going from start to start is zero.
    gScore.put(start, 0.0);
    // For the first node, that value is completely heuristic.
    fScore.put(start, getHeuristicCost(start, null, goal));

    while (!open.isEmpty()) {
      OrientVertex current = open.poll();

      // we discussed about this feature in https://github.com/orientechnologies/orientdb/pull/6002#issuecomment-212492687
      if (paramEmptyIfMaxDepth == true && currentDepth >= paramMaxDepth) {
        route.clear(); // to ensure our result is empty
        return getPath();
      }
      // if start and goal vertex is equal so return current path from  cameFrom hash map
      if (current.getIdentity().equals(goal.getIdentity()) || currentDepth >= paramMaxDepth) {

        while (current != null) {
          route.add(0, current);
          current = cameFrom.get(current);
        }
        return getPath();
      }

      closedSet.add(current);
      for (OrientEdge neighborEdge : getNeighborEdges(current)) {

        OrientVertex neighbor = getNeighbor(current, neighborEdge, graph);
        // Ignore the neighbor which is already evaluated.
        if (closedSet.contains(neighbor)) {
          continue;
        }
        // The distance from start to a neighbor
        double tentative_gScore = gScore.get(current) + getDistance(neighborEdge);
        boolean contains = open.contains(neighbor);

        if (!contains || tentative_gScore < gScore.get(neighbor)) {
          gScore.put(neighbor, tentative_gScore);
          fScore.put(neighbor, tentative_gScore + getHeuristicCost(neighbor, current, goal));

          if (contains) {
            open.remove(neighbor);
          }
          open.offer(neighbor);
          cameFrom.put(neighbor, current);
        }
      }

      // Increment Depth Level
      currentDepth++;

    }

    return getPath();
  }

  public String getSyntax() {
    return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
  }

}