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

import java.util.*;

/**
 * @author Martin Eutinger (m.eutinger a_t googlemail.com)
 * LRTA* based on https://www.iiia.csic.es/sites/default/files/IIIA-2005-1027.pdf
 */
public class OSQLFunctionLRTAstar extends OSQLFunctionAstarAbstract {
    public static final String NAME = "lrtastar";

    protected Set<OrientVertex> closed = new HashSet<OrientVertex>();

    protected Map<OrientVertex, Double> hScore = new HashMap<OrientVertex, Double>();

    public OSQLFunctionLRTAstar() {
        super(NAME, 3, 4);
    }

    protected LinkedList<OrientVertex> internalExecute(final OCommandContext iContext, OrientBaseGraph graph) {

        OrientVertex start = paramSourceVertex;
        OrientVertex goal = paramDestinationVertex;

        // TODO interrupt after timeout PARAM_TIMEOUT
        Map<OrientVertex, Double> old_hScore;
        do
        {
            old_hScore = new HashMap<OrientVertex, Double>(hScore);
            currentDepth = 0;
            closed.clear();
            route.clear();
            route.add(start);
            if(!trial(start, goal)){
                route.clear();
                return getPath();
            }
        } while(!hScore.equals(old_hScore));

        // TODO change; not sure if paramMaxDepth makes sense for lrta
        if (currentDepth >= paramMaxDepth){
            if(paramEmptyIfMaxDepth==true){
                route.clear();
                return getPath();
            }
            LinkedList<OrientVertex> path = new LinkedList<OrientVertex>();
            for(int i = 0; i <= paramMaxDepth; i++)
            {
                path.add(route.get(i));
            }
            return path;
        }

        return getPath();
    }

    // TODO avoid redundant calls
    private boolean trial(OrientVertex start, OrientVertex goal) {
        OrientVertex current = start;
        while(!current.getIdentity().equals(goal.getIdentity()))
        {
            LookaheadUpdate1(current, goal);
            OrientVertex successor = getSuccessor(current, goal);
            if(successor == null){
                return false;
            }
            route.add(successor);
            closed.add(successor);
            current = successor;
            currentDepth++;
        }
        return true;
    }

    private boolean LookaheadUpdate1(OrientVertex current, OrientVertex goal) {
        OrientVertex successor = getSuccessor(current, goal);
        Double successor_hScore = getDistance(current, null, successor) + get_hScore(successor, current, goal);
        if(get_hScore(current, null, goal) < successor_hScore){
            hScore.put(current, successor_hScore);
            return true;
        }
        return false;
    }

    private OrientVertex getSuccessor(OrientVertex current, OrientVertex goal) {
        double pick_hScore = Double.MAX_VALUE;
        OrientVertex pick = null;
        for(OrientVertex neighbor : getNeighbors(current))
        {
            double neighbor_hScore = getDistance(current, null, neighbor) + get_hScore(neighbor, current, goal);
            if(neighbor_hScore < pick_hScore)
            {
                pick_hScore = neighbor_hScore;
                pick = neighbor;
            }
        }
        return pick;
    }

    double get_hScore(final OrientVertex node, OrientVertex parent, final OrientVertex target) {
        Double node_hScore = hScore.get(node);
        if(node_hScore == null)
        {
            return getHeuristicCost(node, parent, target);
        }
        return node_hScore;
    }

    public String getSyntax() {
        return "lrtastar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
    }
}