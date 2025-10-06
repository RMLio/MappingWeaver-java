package be.ugent.idlab.knows;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.Pair;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.amo.functions.FragmentFunction;
import be.ugent.idlab.knows.amo.functions.TargetSink;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.NaturalJoinOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.ThetaJoinOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.ExtendOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.FragmenterOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.ProjectOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.SerializeOperator;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.CSVSourceOperator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.dataio.access.Access;
import be.ugent.idlab.knows.dataio.access.LocalFileAccess;
import be.ugent.idlab.knows.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.mappingplan.join_conditions.EqualityJoinCondition;
import be.ugent.idlab.knows.mappingplan.parsing.Adjacency;
import be.ugent.idlab.knows.utilities.GraphVisitorCustomTarget;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONArray;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

// TODO: these tests need to be checked for proper value implementation

/**
 * Tests showing off simple pipelines
 */
@Disabled
public class MappingPlanTest {

    @SuppressWarnings("null")
    TargetSink<String> sink = value -> {
        System.out.println("Executed");
        assertEquals("Venus", value);
    };

    /**
     * Test running a simple pipeline Source -> Project -> Target
     * The input {name: Venus, age: 10} will get restricted to {name: Venus}, which
     * the target will read out
     */
    @Test
    public void simplePipeline() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // create the operators
        Access access = new LocalFileAccess("mapping_plan/v2/simplePipeline/input.csv", "src/test/resources", "text/csv");
        SourceOperator source = new CSVSourceOperator("Source", access, Set.of("default"), List.of(), List.of());
        ProjectOperator project = new ProjectOperator("Project", Set.of("default"), Set.of("default"), List.of("id", "name"));
        TargetOperator target = new TargetOperator("targetOp", Set.of("default"), "name", sink);
        Adjacency adj = new Adjacency(new JSONArray(
                """
                        [
                            [
                                0,
                                1,
                                {
                                    "fragment": "default",
                                    "direction": "Center"
                                }
                            ],
                            [
                                1,
                                2,
                                {
                                    "fragment": "default",
                                    "direction": "Center"
                                }
                            ]
                        ]
                        """), 3);

        OperatorGraph graph = new OperatorGraph(List.of(source, project, target), adj);

        MappingPlan plan = new MappingPlan(env, graph, new GraphVisitorCustomTarget(env, graph, "name"));

        plan.execute();

        assertTrue(GraphVisitorCustomTarget.ResultCollector.values.contains(new LiteralNode("Venus")));
    }

    /**
     * A test for running two separate pipelines:
     * - Source -> Project -> Target
     * - Source -> Extend -> Target
     * where Source is reused
     */
    @SuppressWarnings("null")
    @Test
    public void twoSeparatePipelines() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        Access access = new LocalFileAccess("mapping_plan/v2/simplePipeline/input.csv", "src/test/resources", "json");
        SourceOperator source1 =  new CSVSourceOperator("Source1", access, Set.of("f_default"), List.of(), List.of());
        SourceOperator source2 = new CSVSourceOperator("Source2", access, Set.of("f_default"), List.of(), List.of());

        // pipeline 1
        ProjectOperator project = new ProjectOperator("Project", Set.of("f_default"), Set.of("f_default"), List.of("id", "name"));
        TargetOperator target = new TargetOperator("targetOp1", Set.of("f_default"), "name", sink);

        // pipeline 2
        ExtendOperator extend = new ExtendOperator("Extend",
                Set.of("f_default"), Set.of("f_default"),
                List.of(new Pair<>("fullname",
                (ExtendFunction) solutionMapping -> "fullname:" + solutionMapping.get("name").getValue())));

        TargetOperator target2 = new TargetOperator("targetOp2", Set.of("f_default"), "fullname",
                (TargetSink<String>) value -> assertEquals("fullname:Venus", value));

        // create the graph
        List<Operator> operators = List.of(source1, project, target, source2, extend, target2);

        boolean[][] adjacency = new boolean[operators.size()][operators.size()];
        adjacency[0][1] = true; // source -> project
        adjacency[1][2] = true; // project -> target
        adjacency[3][4] = true; // source2 -> extend
        adjacency[4][5] = true; // extend -> target2

        OperatorGraph graph = new OperatorGraph(operators, adjacency);

        // create the mapping plan and execute
        MappingPlan plan = new MappingPlan(env, graph, new GraphVisitorCustomTarget(env, graph, "fullname"));
        plan.execute();

        System.out.println(GraphVisitorCustomTarget.ResultCollector.values);
    }

    /**
     * A test to perform a simple join from two pipelines of same length.
     * Data flow:
     * - Source1 -> Join -> Target
     * - Source2 -> Join -> Target
     */
    @SuppressWarnings("null")
    @Test
    public void simpleJoin() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Access input1 = new LocalFileAccess("mapping_plan/v2/join/input1.csv", "src/test/resources", "csv");
        Access input2 = new LocalFileAccess("mapping_plan/v2/join/input2.csv", "src/test/resources", "csv");

        SourceOperator source1 = new CSVSourceOperator("Source1",  input1, Set.of("f_default"), List.of(), List.of());
        SourceOperator source2 =  new CSVSourceOperator("Source2",  input2, Set.of("f_default"), List.of(), List.of());

        Map<String,String> condition =  new HashMap<>(); 
        condition.put("player_id", "id"); 

        ThetaJoinOperator join = new ThetaJoinOperator("Theta1", Set.of("f_default"), Set.of("f_default"),
                new EqualityJoinCondition(condition));

        TargetOperator target = new TargetOperator("targetOp", Set.of("f_default"), "score", (TargetSink<String>) value -> {
            System.out.println("Target received " + value);
            assertEquals("20", value);
        });

        List<Operator> operators = List.of(source1, source2, join, target);

        Adjacency adj = new Adjacency(new JSONArray("""
                    [
                        [
                            0,
                            2,
                            {
                                "fragment": "f_default",
                                "direction": "Left"

                            }

                        ],
                        [
                            1,
                            2,
                            {
                                "fragment": "f_default",
                                "direction": "Right"

                            }

                        ],

                        [
                            2,
                            3,
                            {
                                "fragment": "f_default",
                                "direction": "Center"

                            }

                        ]

                    ]


                """), 4);

        OperatorGraph graph = new OperatorGraph(operators, adj);

        MappingPlan plan = new MappingPlan(env, graph, new GraphOpVisitor(env, graph));

        plan.execute();
    }

    /**
     * Tests the fragmentation into different sinks. Values for "id" and "name" stay
     * in f_default, values for "sport" and "score" go into f_new
     * Data flow:
     * - Source -> Fragment -> Target: f_default
     * - Source -> Fragment -> Target: f_new
     */
    @SuppressWarnings("null")
    @Test
    public void fragmentIntoDifferentSinks() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Access access = new LocalFileAccess("mapping_plan/v2/fragment/input.csv", "src/test/resources", "csv");
        SourceOperator source = new CSVSourceOperator("Source",  access, Set.of("f_default"), List.of(), List.of());

        FragmentFunction fragmentFunction = (mappingTuple) -> {
            MappingTuple out = new MappingTuple();
            Collection<SolutionMapping> mappings = mappingTuple.getSolutionMappings("f_default");
            Collection<SolutionMapping> mappingsDefault = mappings.stream()
                    .filter(sm -> sm.containsKey("id") || sm.containsKey("name")).toList();
            Collection<SolutionMapping> mappingsNew = mappings.stream()
                    .filter(sm -> sm.containsKey("sport") || sm.containsKey("score")).toList();

            out.setSolutionMaps("f_default", mappingsDefault);
            out.setSolutionMaps("f_new", mappingsNew);

            return out;
        };

        FragmenterOperator fragment = new FragmenterOperator("Fragment", Set.of("f_default"), Set.of("f_default", "f_new"), fragmentFunction);

        TargetOperator targetDefault = new TargetOperator("targetOp1", Set.of("f_default"), "sport",
                value -> assertEquals("Tennis", value));

        TargetOperator targetNew = new TargetOperator("targetOp2", Set.of("f_new"), "name",
                value -> assertEquals("Venus", value));
        List<Operator> operators = List.of(source, fragment, targetDefault, targetNew);
        boolean[][] adjacency = new boolean[operators.size()][operators.size()];
        adjacency[0][1] = true;
        adjacency[1][2] = true;
        adjacency[1][3] = true;

        OperatorGraph graph = new OperatorGraph(List.of(source, fragment, targetDefault, targetNew), adjacency);

        MappingPlan plan = new MappingPlan(env, graph, new GraphOpVisitor(env, graph));
        plan.execute();
    }

    /**
     * Tests correct calculation of topological order on a more involved graph
     * Test input has been inspired by teh test case 0008b,
     * visualised <a href=
     * "https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/blob/main/resources/csv-testcases/RMLTC0008b-CSV/pretty.png?ref_type=heads">here</a>.
     */
    @Test
    public void topologicalOrderTest() {
        // we simply need the operators as blocks, not their functionality: give them
        // dummy arguments
        SourceOperator source_0 = new CSVSourceOperator("source_0", null,  Set.of("f_default"), List.of(), List.of());
        ProjectOperator projection_1 = new ProjectOperator("project_1", Set.of("f_default"), Set.of("f_default"), null);
        SourceOperator source_2 = new CSVSourceOperator("source_2", null,  Set.of("f_default"), List.of(), List.of());
        ProjectOperator projection_3 = new ProjectOperator("project_3", Set.of("f_default"), Set.of("f_default"), null);
        FragmenterOperator fragmenter_4 = new FragmenterOperator("fragment_4", Set.of("f_default"), Set.of("f_default"), null);
        FragmenterOperator fragmenter_5 = new FragmenterOperator("fragment_5", Set.of("f_default"), Set.of("f_default"), null);
        NaturalJoinOperator join_6 = new NaturalJoinOperator("join_6", Set.of("f_default"), Set.of("f_default"));
        ExtendOperator extend_7 = new ExtendOperator("extend_7", Set.of("f_default"), Set.of("f_default"), List.of());
        SerializeOperator serialize_8 = new SerializeOperator("serialize_8", Set.of("f_default"), Set.of("f_default"), null, null);
        TargetOperator sink_9 = new TargetOperator("sink_9", Set.of("f_default"), null, null);
        ExtendOperator extend_10 = new ExtendOperator("extend_10", Set.of("f_default"), Set.of("f_default"), List.of());
        SerializeOperator serialize_11 = new SerializeOperator("serialize_11", Set.of("f_default"), Set.of("f_default"), null, null);
        TargetOperator sink_12 = new TargetOperator("sink_12", Set.of("f_default"), null, null);
        ExtendOperator extend_13 = new ExtendOperator("extend_13", Set.of("f_default"), Set.of("f_default"), List.of());
        SerializeOperator serialize_14 = new SerializeOperator("serialize_14", Set.of("f_default"), Set.of("f_default"), null, null);
        TargetOperator sink_15 = new TargetOperator("sink_15", Set.of("f_default"), null, null);

        List<Operator> operators = List.of(source_0, projection_1, source_2, projection_3, fragmenter_4, fragmenter_5,
                join_6, extend_7, serialize_8, sink_9, extend_10, serialize_11, sink_12, extend_13, serialize_14,
                sink_15);

        // set up connections
        // these edges are written in a depth-first-search fashion, from sources to
        // sinks
        boolean[][] adjacency = new boolean[operators.size()][operators.size()];
        adjacency[0][1] = true;
        adjacency[1][4] = true;
        adjacency[4][13] = true;
        adjacency[13][14] = true;
        adjacency[14][15] = true;
        adjacency[4][6] = true;
        adjacency[6][7] = true;
        adjacency[7][8] = true;
        adjacency[8][9] = true;
        adjacency[2][3] = true;
        adjacency[3][5] = true;
        adjacency[5][6] = true;
        adjacency[5][10] = true;
        adjacency[10][11] = true;
        adjacency[11][12] = true;

        OperatorGraph graph = new OperatorGraph(operators, adjacency);

        List<Operator> topoOrder = graph.topologicalOrder();
        System.out.println(topoOrder);

        // assert that the topological order is valid
        // topological order is not unique, therefore logical expressions need to be
        // utilized.
        // Only direct connections between operators are verified, as we can use
        // transitive property of < to ensure that,
        // for example, fragmenter_4 comes before serialize_14, since fragmenter_4 comes
        // before extend_13 which comes before serialize_14
        // these conditions are written following a depth-first search of the graph from
        // the bottom to the top, from sinks to sources
        assertTrue(topoOrder.indexOf(serialize_14) < topoOrder.indexOf(sink_15));
        assertTrue(topoOrder.indexOf(extend_13) < topoOrder.indexOf(serialize_14));
        assertTrue(topoOrder.indexOf(fragmenter_4) < topoOrder.indexOf(extend_13));
        assertTrue(topoOrder.indexOf(projection_1) < topoOrder.indexOf(fragmenter_4));
        assertTrue(topoOrder.indexOf(source_0) < topoOrder.indexOf(projection_1));
        assertTrue(topoOrder.indexOf(serialize_8) < topoOrder.indexOf(sink_9));
        assertTrue(topoOrder.indexOf(extend_7) < topoOrder.indexOf(serialize_8));
        assertTrue(topoOrder.indexOf(join_6) < topoOrder.indexOf(extend_7));
        assertTrue(topoOrder.indexOf(fragmenter_5) < topoOrder.indexOf(join_6));
        assertTrue(topoOrder.indexOf(projection_3) < topoOrder.indexOf(fragmenter_5));
        assertTrue(topoOrder.indexOf(source_2) < topoOrder.indexOf(projection_3));
        assertTrue(topoOrder.indexOf(serialize_11) < topoOrder.indexOf(sink_12));
        assertTrue(topoOrder.indexOf(extend_10) < topoOrder.indexOf(serialize_11));
        assertTrue(topoOrder.indexOf(fragmenter_5) < topoOrder.indexOf(extend_10));
    }
}
