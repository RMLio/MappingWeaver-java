package be.ugent.idlab.knows.mappingplan.parsing;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Describes the adjacency between nodes: what nodes are connected to what nodes
 * and which fragments should the operators work on
 */
public class Adjacency {

    public record Fragment(String name, String direction) {
    }

    // internal adjacency between parents and children, storing the respective
    // indexes
    // TODO: rework into an array
    private final Map<Integer, List<Integer>> parents = new HashMap<>();
    private final Map<Integer, List<Integer>> children = new HashMap<>();
    private final boolean[][] connections;
    private final Fragment[][] fragments;

    public Adjacency(JSONArray adjacencyInfo, int operatorCount) {
        for (int i = 0; i < operatorCount; i++) {
            this.parents.put(i, new ArrayList<>());
            this.children.put(i, new ArrayList<>());
        }

        this.connections = new boolean[operatorCount][operatorCount];
        this.fragments = new Fragment[operatorCount][operatorCount];
        for (int i = 0; i < adjacencyInfo.length(); i++) {
            JSONArray edge = adjacencyInfo.getJSONArray(i);
            int from = edge.getInt(0);
            int to = edge.getInt(1);
            JSONObject fragmentObject = edge.getJSONObject(2);
            Fragment fragment = new Fragment(fragmentObject.getString("fragment"),
                    fragmentObject.getString("direction"));

            this.connections[from][to] = true;
            this.fragments[from][to] = fragment;
            this.parents.get(to).add(from);
            this.children.get(from).add(to);
        }
    }

    /**
     * Function to determine what fragment to run the operator on: what fragment is
     * entering into the operator
     *
     * @param opIndex index of the operator in the operator list
     * @return the name of the fragment
     */
    public Set<Fragment> getIncomingFragments(int opIndex) {
        Set<Fragment> result = new HashSet<>();
        List<Integer> parents = this.parents.get(opIndex);
        for (Integer parent : parents) {
            result.add(this.fragments[parent][opIndex]);
        }
        return result;
    }

    /**
     * Function to determine what fragment is leaving the operator: what fragment
     * the operator writes to
     *
     * @param opIndex index of the operator in the operator list
     * @return the name of the fragment
     */
    public Set<Fragment> getOutgoingFragments(int opIndex) {
        Set<Fragment> result = new HashSet<>();
        List<Integer> children = this.children.get(opIndex);
        for (Integer child : children) {
            result.add(this.fragments[opIndex][child]);
        }
        return result;

    }

    public boolean[][] getConnections() {
        return this.connections;
    }

    public Fragment[][] getFragments() {
        return this.fragments;

    }

    public Map<Integer, List<Integer>> getParents() {
        return this.parents;
    }

    public Map<Integer, List<Integer>> getChildren() {
        return this.children;
    }
}
