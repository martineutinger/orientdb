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
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;

/**
 * @author Saeed Tabrizi (saeed a_t  nowcando.com) s. OSQLFunctionAstar
 */
public class OSQLFunctionIDAstar extends OSQLFunctionHeuristicPathFinderAbstract {
    public static final String NAME = "idastar";

    private String paramWeightFieldName = "weight";
    private long currentDepth = 0;
    protected Map<OrientVertex, OrientVertex> cameFrom = new HashMap<OrientVertex, OrientVertex>();

    protected Map<OrientVertex, Double> gScore = new HashMap<OrientVertex, Double>();

    public OSQLFunctionIDAstar() {
        super(NAME, 3, 4);
    }

    public LinkedList<OrientVertex> execute(final Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult,
                                            final Object[] iParams, final OCommandContext iContext) {
        context = iContext;
        final OSQLFunctionIDAstar context = this;
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
                        return internalExecute(iContext);
                    }
                });
    }

    private LinkedList<OrientVertex> internalExecute(final OCommandContext iContext) {

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
            if (paramEmptyIfMaxDepth==true && result.getDepth() >= paramMaxDepth){
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
            if(isOnPath(neighbor, current)){
                continue;
            }
            cameFrom.put(neighbor, current);
            gScore.put(neighbor, gScore.get(current) + getDistance(current, current, neighbor));
            IDAStarResult result = search(neighbor, current, goal, limit, depth+1);
            if (result.getSolutionVertex() != null) {
                return result;
            }
            if (result.getLimit() < minLimit) {
                minLimit = result.getLimit();
            }
            if (result.getDepth() > maxDepth)
            {
                maxDepth = result.getDepth();
            }
        }
        return new IDAStarResult(minLimit, maxDepth);
    }

    private boolean isOnPath(OrientVertex check, OrientVertex current) {
        while(current != null)
        {
            if(current.getIdentity().equals(check.getIdentity()))
            {
                return true;
            }
            current = cameFrom.get(current);
        }
        return false;
    }

    private class IDAStarResult {
        private OrientVertex solutionVertex;
        private double limit;
        private long depth;


        public IDAStarResult(OrientVertex solutionVertex){
            this.solutionVertex = solutionVertex;
        }

        public IDAStarResult(double limit){
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

    private void bindAdditionalParams(Object additionalParams, OSQLFunctionIDAstar ctx) {
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
            ctx.paramEdgeTypeNames = stringArray(mapParams.get(OSQLFunctionIDAstar.PARAM_EDGE_TYPE_NAMES));
            ctx.paramVertexAxisNames = stringArray(mapParams.get(OSQLFunctionIDAstar.PARAM_VERTEX_AXIS_NAMES));
            if(mapParams.get(OSQLFunctionIDAstar.PARAM_DIRECTION) != null){
                if (mapParams.get(OSQLFunctionIDAstar.PARAM_DIRECTION) instanceof  String){
                    ctx.paramDirection = Direction.valueOf(stringOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_DIRECTION), "OUT").toUpperCase());
                }
                else{
                    ctx.paramDirection = (Direction) mapParams.get(OSQLFunctionIDAstar.PARAM_DIRECTION);
                }
            }


            ctx.paramParallel = booleanOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_PARALLEL), false);
            ctx.paramMaxDepth = longOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_MAX_DEPTH), ctx.paramMaxDepth);
            ctx.paramEmptyIfMaxDepth = booleanOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_EMPTY_IF_MAX_DEPTH), ctx.paramEmptyIfMaxDepth);
            ctx.paramTieBreaker = booleanOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_TIE_BREAKER), ctx.paramTieBreaker);
            ctx.paramDFactor = doubleOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_D_FACTOR), ctx.paramDFactor);
            if(mapParams.get(OSQLFunctionIDAstar.PARAM_HEURISTIC_FORMULA) !=null){
                if (mapParams.get(OSQLFunctionIDAstar.PARAM_HEURISTIC_FORMULA) instanceof  String){
                    ctx.paramHeuristicFormula = HeuristicFormula.valueOf(stringOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_HEURISTIC_FORMULA), "MANHATAN").toUpperCase());
                }
                else{
                    ctx.paramHeuristicFormula = (HeuristicFormula)mapParams.get(OSQLFunctionIDAstar.PARAM_HEURISTIC_FORMULA);
                }
            }


            ctx.paramCustomHeuristicFormula = stringOrDefault(mapParams.get(OSQLFunctionIDAstar.PARAM_CUSTOM_HEURISTIC_FORMULA), "");
        }
    }


    public String getSyntax() {
        return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
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
            double n = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]),0.0) ;
            double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]),0.0);
            hresult = getSimpleHeuristicCost(n, g, paramDFactor);
        } else if (paramVertexAxisNames.length == 2) {
            if (parent == null) parent = node;
            double sx = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[0]),0);
            double sy = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[1]),0);
            double nx = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]),0);
            double ny = doubleOrDefault(node.getProperty(paramVertexAxisNames[1]),0);
            double px = doubleOrDefault(parent.getProperty(paramVertexAxisNames[0]),0);
            double py = doubleOrDefault(parent.getProperty(paramVertexAxisNames[1]),0);
            double gx = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]),0);
            double gy = doubleOrDefault(target.getProperty(paramVertexAxisNames[1]),0);

            switch (paramHeuristicFormula) {
                case MANHATAN:
                    hresult = getManhatanHeuristicCost(nx, ny, gx, gy, paramDFactor);
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
                case CUSTOM:
                    hresult = getCustomHeuristicCost(paramCustomHeuristicFormula,paramVertexAxisNames,paramSourceVertex,paramDestinationVertex, node, parent,currentDepth, paramDFactor);
                    break;
            }
            if (paramTieBreaker) {
                hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
            }

        } else {
            Map<String,Double> sList = new HashMap<String,Double>();
            Map<String,Double> cList = new HashMap<String,Double>();
            Map<String,Double> pList = new HashMap<String,Double>();
            Map<String,Double> gList = new HashMap<String,Double>();
            parent = parent == null ? node : parent;
            for (int i = 0; i < paramVertexAxisNames.length; i++) {
                Double s = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[i]),0);
                Double c = doubleOrDefault(node.getProperty(paramVertexAxisNames[i]),0);
                Double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[i]),0);
                Double p = doubleOrDefault(parent.getProperty(paramVertexAxisNames[i]),0);
                if (s != null)
                    sList.put(paramVertexAxisNames[i],s);
                if (c != null)
                    cList.put(paramVertexAxisNames[i],s);
                if (g != null)
                    gList.put(paramVertexAxisNames[i],g);
                if (p != null)
                    pList.put(paramVertexAxisNames[i],p);
            }
            switch (paramHeuristicFormula) {
                case MANHATAN:
                    hresult = getManhatanHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case MAXAXIS:
                    hresult = getMaxAxisHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case DIAGONAL:
                    hresult = getDiagonalHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case EUCLIDEAN:
                    hresult = getEuclideanHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case EUCLIDEANNOSQR:
                    hresult = getEuclideanNoSQRHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth,paramDFactor);
                    break;
                case CUSTOM:
                    hresult = getCustomHeuristicCost(paramCustomHeuristicFormula,paramVertexAxisNames,paramSourceVertex,paramDestinationVertex, node, parent,currentDepth, paramDFactor);
                    break;
            }
            if (paramTieBreaker) {
                hresult = getTieBreakingHeuristicCost(paramVertexAxisNames,sList,cList,pList,gList,currentDepth, hresult);
            }



        }


        return hresult;

    }


    @Override
    protected boolean isVariableEdgeWeight() {
        return true;
    }

}