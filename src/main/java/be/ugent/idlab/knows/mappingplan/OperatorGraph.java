package be.ugent.idlab.knows.mappingplan;

import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.mappingplan.parsing.Adjacency;
import be.ugent.idlab.knows.mappingplan.parsing.Adjacency.Fragment;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Represents the Directed-Acyclic-Graph (DAG) of the operator pipeline
 */
public class OperatorGraph {

    private final boolean[][] connections;
    private final Fragment[][] fragments;
    private final Map<Operator, List<FragmentOperatorPair>> parents = new HashMap<>();
    private final Map<Operator, List<FragmentOperatorPair>> children = new HashMap<>();
    private final List<Operator> operators;
    private final Adjacency adjacency;

    /**
     * Constructs a new OperatorGraph.
     *
     * @param operators operators in the pipeline
     * @param adjacency adjacency matrix of the operators
     */
    public OperatorGraph(List<Operator> operators, Adjacency adjacency) {

        this.operators = operators;
        this.connections = adjacency.getConnections();
        this.fragments = adjacency.getFragments();
        this.adjacency = adjacency;
    }

    public OperatorGraph(List<Operator> operators, boolean[][] adjacency) {
        this.operators = operators;
        this.connections = adjacency;
        this.fragments = new Fragment[operators.size()][operators.size()];
        this.adjacency = null;
        this.calculateParentChildInformation();
    }

    private void calculateParentChildInformation() {
        for (Operator op : this.operators) {
            this.parents.put(op, new ArrayList<>());
            this.children.put(op, new ArrayList<>());
        }

        for (int i = 0; i < this.operators.size(); i++) {
            Operator parent = this.operators.get(i);

            for (int j = 0; j < this.operators.size(); j++) {
                if (this.connections[i][j]) {
                    Operator child = this.operators.get(j);
                    this.parents.get(child).add(new FragmentOperatorPair(null , parent));
                    this.children.get(parent).add(new FragmentOperatorPair(null, child));

                }

            }
        }

    }

    /**
     * Calculates the information of the graph based on the adjacency matrix
     */
    private void calculateParentChildInformationAdj(Adjacency adjacency) {

        for (Map.Entry<Integer, List<Integer>> entry : adjacency.getParents().entrySet()) {

            Operator child = this.operators.get(entry.getKey());
            List<FragmentOperatorPair> parents = entry.getValue().stream().map((idx) ->

                            new FragmentOperatorPair(
                                    this.fragments[idx][entry.getKey()],
                                    this.operators.get(idx)))
                    .collect(Collectors.toList());
            this.parents.put(child, parents);
        }

        for (Map.Entry<Integer, List<Integer>> entry : adjacency.getChildren().entrySet()) {
            Operator parent = this.operators.get(entry.getKey());
            List<FragmentOperatorPair> children = entry.getValue().stream().map((idx) -> new FragmentOperatorPair(
                            this.fragments[entry.getKey()][idx],
                            this.operators.get(idx)))
                    .collect(Collectors.toList());
            this.children.put(parent, children);
        }

    }

    public List<Operator> getOperators() {
        return operators;
    }

    public List<FragmentOperatorPair> getParents(Operator op) {
        return this.parents.get(op);
    }

    public List<FragmentOperatorPair> getChildren(Operator op) {
        return this.children.get(op);
    }

    public List<Operator> topologicalOrder() {
        if (this.adjacency != null) {
            this.calculateParentChildInformationAdj(adjacency);
        } else {
            this.calculateParentChildInformation();
        }

        // take a copy of the connection information
        Map<Operator, List<Operator>> parents = new HashMap<>();
        for (Map.Entry<Operator, List<FragmentOperatorPair>> e : this.parents.entrySet()) {
            List<Operator> operators = e.getValue().stream().map((pair) -> pair.operator())
                    .collect(Collectors.toList());
            parents.put(e.getKey(), operators);
        }

        Map<Operator, List<Operator>> children = new HashMap<>();
        for (Map.Entry<Operator, List<FragmentOperatorPair>> e : this.children.entrySet()) {
            List<Operator> operators = e.getValue().stream().map((pair) -> pair.operator())
                    .collect(Collectors.toList());
            children.put(e.getKey(), operators);
        }

        // output list with the topological order of operators
        List<Operator> out = new ArrayList<>();

        // set of all operators with no incoming edges
        // in the beginning, all sources are such operators
        Queue<Operator> noParent = new ArrayDeque<>(
                this.operators.stream().filter(op -> op instanceof SourceOperator).toList());

        if (noParent.isEmpty()) {
            noParent.add(this.operators.getFirst());
        }

        while (!noParent.isEmpty()) {
            Operator op = noParent.poll();
            out.add(op);

            List<Operator> opChildren = new ArrayList<>(children.get(op));
            for (Operator child : opChildren) {
                // remove the connection between op and child
                parents.get(child).remove(op);
                children.get(op).remove(child);

                if (parents.get(child).isEmpty()) {
                    noParent.offer(child);
                }
            }
        }

        return out;
    }

    public record FragmentOperatorPair(Fragment fragment, Operator operator) {
    }
}
