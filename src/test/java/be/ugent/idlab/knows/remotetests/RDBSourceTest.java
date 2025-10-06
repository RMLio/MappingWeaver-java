package be.ugent.idlab.knows.remotetests;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import be.ugent.idlab.knows.utilities.GraphVisitorCustomTargetDebugger;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingplan.MappingPlan;
import be.ugent.idlab.knows.cores.TestCore;
import be.ugent.idlab.knows.utilities.FlinkMiniClusterExtension;
import be.ugent.idlab.knows.utilities.GraphVisitorCustomTarget;

@ExtendWith(FlinkMiniClusterExtension.class)
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class RDBSourceTest {

    // These are almost all tests that require the type out of the database.
    private static List<String> unfixableTests = List.of("RMLTC0016c-MySQL", "RMLTC0016d-MySQL", "RMLTC0016e-MySQL");
    private static List<String> untranslatedTests = List.of("RMLTC0007h-MySQL",   "RMLTC0011a-MySQL");

    private static Stream<Arguments> errorTests() {
        List<String> directories = List.of("RMLTC0005a-MySQL");
        return directories.stream().map(Arguments::of);
    }


    // Test cases for the RML test cases, the name is the respective folder name.
    // If there is no output.nq file, the test is expected to throw an exception.

    private static Stream<Arguments> rmlTestCases() {
        // Define a map with positive and negative test cases
        Map<String, List<String>> testCases = Map.of(
                "positive", List.of(
                        "RMLTC0000-MySQL",
                        "RMLTC0001a-MySQL",
                        "RMLTC0001b-MySQL",
                        "RMLTC0002a-MySQL",
                        "RMLTC0002b-MySQL",
                        "RMLTC0002d-MySQL",
                        "RMLTC0002i-MySQL",
                        //"RMLTC0002j-MySQL", Old SQL syntaxis
                        "RMLTC0003b-MySQL",
                        "RMLTC0003c-MySQL",
                        "RMLTC0004a-MySQL",
                        "RMLTC0005a-MySQL",
                        "RMLTC0005b-MySQL",
                        "RMLTC0006a-MySQL",
                        "RMLTC0007a-MySQL",
                        "RMLTC0007b-MySQL",
                        "RMLTC0007c-MySQL",
                        "RMLTC0007d-MySQL",
                        "RMLTC0007e-MySQL",
                        "RMLTC0007f-MySQL",
                        "RMLTC0007g-MySQL",
                        "RMLTC0008a-MySQL",
                        "RMLTC0008b-MySQL",
                        "RMLTC0008c-MySQL",
                        "RMLTC0009a-MySQL",
                        "RMLTC0009b-MySQL",
                        "RMLTC0009c-MySQL",
                        "RMLTC0009d-MySQL",
                        "RMLTC0010a-MySQL",
                        "RMLTC0010b-MySQL",
                        "RMLTC0010c-MySQL",
                        "RMLTC0011b-MySQL",
                        "RMLTC0012a-MySQL",
                        "RMLTC0012b-MySQL",
                        "RMLTC0012e-MySQL",
                        "RMLTC0013a-MySQL",
                        "RMLTC0014d-MySQL",
                        "RMLTC0015a-MySQL", // 15a was translated to new sql, due to the RDBAccess not supporting old SQL syntaxis.
                        "RMLTC0016a-MySQL",
                        "RMLTC0016b-MySQL",
                        "RMLTC0018a-MySQL",
                        "RMLTC0019a-MySQL",
                        "RMLTC0019b-MySQL",
                        "RMLTC0020a-MySQL",
                        "RMLTC0020b-MySQL"
                ),
                "negative", List.of(
                        "RMLTC0002c-MySQL",
                        "RMLTC0002e-MySQL", // potentially look at translation for this one
                        "RMLTC0002f-MySQL",
                        "RMLTC0002g-MySQL", // invalid sql query
                        "RMLTC0002h-MySQL",
                        "RMLTC0002e-MySQL",
                        "RMLTC0004b-MySQL",
                        "RMLTC0012c-MySQL",
                        "RMLTC0012d-MySQL",
                        "RMLTC0015b-MySQL",
                        "RMLTC0017a-MySQL",
                        "RMLTC0017b-MySQL"  // invalid sql query
                )
        );

        // Combine positive and negative test cases into a single stream
        return Stream.concat(
                testCases.get("positive").stream().map(testCase -> Arguments.of("positive", testCase)),
                testCases.get("negative").stream().map(testCase -> Arguments.of("negative", testCase))
        );
    }

    private boolean resetDatabase(String databaseName) {
        String dbUrl = "jdbc:mysql://localhost:3306?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String username = "root";
        String password = "";

        try (Connection conn = DriverManager.getConnection(dbUrl, username, password)) {
            Statement statement = conn.createStatement();

            // Drop the database if it exists
            String dropSQL = "DROP DATABASE IF EXISTS " + databaseName;
            statement.executeUpdate(dropSQL);

            // Create the database again
            String createSQL = "CREATE DATABASE " + databaseName;
            statement.executeUpdate(createSQL);

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }


    private boolean prepareDatabase(String path) {
        String dbUrl = "jdbc:mysql://localhost:3306/testing?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC";
        String username = "root";
        String password = "";

        try (Connection conn = DriverManager.getConnection(dbUrl, username, password)) {
            ScriptRunner runner = new ScriptRunner(conn);
            try (Reader reader = new BufferedReader(new FileReader(path))) {
                runner.runScript(reader);
            }
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    private Quad doubleToScientific(Quad quad) {
        String objectString = quad.getObject().toString();
        if (objectString.contains("^^http://www.w3.org/2001/XMLSchema#double")) {
            // Extract the numeric part before the datatype declaration
            String numericPart = objectString.split("\\^\\^")[0].replaceAll("\"", "");

            try {
                // Convert the numeric part to a double
                double value = Double.parseDouble(numericPart);

                // Create a new literal with the formatted value and retain the datatype
                Node modifiedNode = NodeFactory.createLiteral(String.valueOf(value), XSDDatatype.XSDdouble);

                // Return a new Quad with the modified node, retaining the original subject, predicate, and graph
                return Quad.create(quad.getGraph(), quad.getSubject(), quad.getPredicate(), modifiedNode);
            } catch (NumberFormatException e) {
                // Handle any parsing issues gracefully (e.g., if the string is not a valid double)
                e.printStackTrace();
            }
        }
        return quad;
    }


    @Test
    public void debugOne() throws Exception {
        sqlTest("positive", "RMLTC001a-MySQL", true);
    }

    @ParameterizedTest
    @MethodSource("rmlTestCases")
    void sqlTest(String testType, String directory) throws Exception {
        sqlTest(testType, directory, false);
    }


    void sqlTest(String testType, String directory, boolean debug) throws Exception {
        System.out.println("Starting test: " + directory);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        GraphVisitorCustomTargetDebugger.ResultCollector.values.clear();

        String basePath = "src/test/resources/test-cases/mysql/";
        String planPath = directory + "/mapping.json";
        String resourcePath = basePath + directory + "/resource.sql";
        resetDatabase("test");
        if (!prepareDatabase(resourcePath)) {
            return; // Skip faulty tests
        }

        MappingPlan plan = MappingPlan.fromFile(env, basePath + planPath);
        GraphVisitorCustomTargetDebugger visitor = new GraphVisitorCustomTargetDebugger(env, plan.getOperatorGraph(), TargetOperator.TARGET_VARIABLE);
        visitor.activateDebugging();
        plan.setVisitor(visitor);



        if(unfixableTests.contains(directory) || untranslatedTests.contains(directory)) {
            System.out.println("unfixable test/ non existant test case: " + directory);
            return;
        }

        if(testType.equals("positive")) {
             testPositive(plan, directory, basePath + directory + "/output.nq");
        }else {
            testError(plan, directory);
        }
    }


    private void testPositive(MappingPlan plan, String directory, String expectedPath) throws Exception {

        // execute plan
        plan.execute();

        // get expected output
        List<Quad> expected = new ArrayList<>();
        TestCore.getDatasetGraph(new File(expectedPath)).find().forEachRemaining(expected::add);

        // get actual output
        StringBuilder sb = new StringBuilder();
        for (String rdf : GraphVisitorCustomTarget.ResultCollector.values) {
            sb.append(rdf).append("\n");
        }

        List<Quad> actual = new ArrayList<>();
        TestCore.getDatasetGraph(sb.toString()).find().forEachRemaining(actual::add);

        // format doubles the same way
        actual.replaceAll(this::doubleToScientific);
        expected.replaceAll(this::doubleToScientific);

        Assertions.assertEquals(expected, actual,  String.format("Failed test case: %s", directory));
    }

    private void testError(MappingPlan plan, String directory)  {
        // Execute the plan and ensure it either throws an exception or produces no output
        try {
            plan.execute();
        } catch (Exception e) {
            System.out.printf("Test passed due to exception: %s%n", directory);
            return; // If an exception is thrown, the test passes.
        }

        // Check if there are no results in the result collector, which would mean no output was produced
        if (GraphVisitorCustomTarget.ResultCollector.values.isEmpty()) {
            System.out.printf("Test passed: No output generated as expected for %s%n", directory);
            return; // No output means the test passes.
        }

        // If there is output, gather the actual results
        StringBuilder sb = new StringBuilder();
        for (String rdf : GraphVisitorCustomTarget.ResultCollector.values) {
            sb.append(rdf).append("\n");
        }

        List<Quad> actual = new ArrayList<>();
        TestCore.getDatasetGraph(sb.toString()).find().forEachRemaining(actual::add);

        // Ensure the actual output matches the expectation of no results
        Assertions.assertEquals(new ArrayList<>(), actual,String.format("Failed test case: Unexpected output for %s", directory));
    }


}
