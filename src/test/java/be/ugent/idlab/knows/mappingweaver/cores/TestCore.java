package be.ugent.idlab.knows.mappingweaver.cores;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.dataio.utils.NAMESPACES;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import be.ugent.idlab.knows.mappingweaver.utilities.GraphVisitorCustomTarget;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.SerializedThrowable;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.FactoryRDFStd;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(FlinkMiniClusterExtension.class)
public abstract class TestCore {

    /**
     * Provides a DatasetGraph from an RDF String
     *
     * @param rdf rdf to consume
     * @return a DatasetGraph representing the RDF string
     */
    public static DatasetGraph getDatasetGraph(String rdf) {
        return getDatasetGraph(new ByteArrayInputStream(rdf.getBytes()));
    }

    /**
     * Provides a DatasetGraph from a File object containing the RDF in NQ format
     *
     * @param file file to process
     * @return a DatasetGraph representing the NQ file
     */
    public static DatasetGraph getDatasetGraph(File file) throws FileNotFoundException {
        return getDatasetGraph(new FileInputStream(file));
    }

    public static DatasetGraph getDatasetGraph(InputStream is) {
        return RDFParser.create()
                .factory(new FactoryRDFStd(LabelToNode.createUseLabelAsGiven())) // ensure that blank labels are used as
                // is
                .source(is)
                .lang(Lang.NQ)
                .toDatasetGraph();
    }

    public void positiveTest(String basePath, String directory, boolean bestEffort) throws Exception {
        this.runTest(basePath, directory, true, bestEffort);
    }

    public void positiveTest(String basePath, String directory, String mappingPlan, boolean bestEffort) throws FileNotFoundException {
        this.runTestWithMappingPlan(basePath, directory, mappingPlan, true, bestEffort);
    }

    public void positiveTestTurtlePlan(String basePath, String directory, String turtlePlan, boolean bestEffort) throws FileNotFoundException {
        String mappingPlan = ITranslator.getInstance().translate_to_document(turtlePlan);

        this.runTestWithMappingPlan(basePath, directory, mappingPlan, true, bestEffort);
    }

    public void negativeTest(String basePath, String directory, boolean bestEffort) throws Exception {
        this.runTest(basePath, directory, false, bestEffort);
    }

    public void negativeTestTurtlePlan(String basePath, String directory, String turtlePlan, boolean bestEffort) throws FileNotFoundException {
        String mappingPlan = ITranslator.getInstance().translate_to_document(turtlePlan);

        this.runTestWithMappingPlan(basePath, directory, mappingPlan, false, bestEffort);
    }

    private void runTest(String basePath, String directory, boolean positive, boolean bestEffort) throws IOException {
        String plan = null;
        try {
            plan = this.getMappingPlan(basePath, directory);
            // Write to test dir to see what plan gets generated
            Files.writeString(Path.of(basePath, directory, "mapping.json"), plan, StandardCharsets.UTF_8);
        } catch (Throwable t) {
            if (positive) {
                System.err.println("Error: " + t.getMessage().replace('|', '\n'));
                fail("Positive test shouldn't fail!");
            } else {
                return;
            }
        }
        runTestWithMappingPlan(basePath, directory, plan, positive, bestEffort);
    }

    private String getMappingPlan(String basePath, String directory) throws IOException {
        String mapping = Files.readString(Paths.get(basePath, directory, "mapping.ttl"));
        ITranslator translator = ITranslator.getInstance();

        String mappingPlan = translator.translate_to_document(mapping);
        //System.out.println(mappingPlan);
        return mappingPlan;
    }


    private void runTestWithMappingPlan(String basePath, String directory, String mappingPlan, boolean positive, boolean bestEffort) throws FileNotFoundException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        String resultSetId = UUID.randomUUID().toString();
        GraphVisitorCustomTarget.ResultCollector.initialize(resultSetId);
//        System.out.println(mappingPlan);

        System.out.printf("Testing test case: %s%n", directory);
        try {
            MappingPlan plan = MappingPlan.fromString(env, mappingPlan, Paths.get(basePath, directory).toString(), "http://example.com/", bestEffort);
//            System.out.println(plan.getOperatorGraph().getOperators());
            GraphOpVisitor visitor = new GraphVisitorCustomTarget(env, plan.getOperatorGraph(),
                    TargetOperator.TARGET_VARIABLE, resultSetId);
            plan.setVisitor(visitor);

            plan.initializeFlinkTopology(true);
            plan.execute();
        } catch (Throwable e) {
            if (positive) { // if positive, rethrow the exception to cause the test to fail
                GraphVisitorCustomTarget.ResultCollector.remove(resultSetId);
                throw new RuntimeException(e);
            } else {
                Throwable current = e;
                // scan the exception stack for a MappingException
                String className;
                do  {
                    if (current instanceof SerializedThrowable) {
                        className = ((SerializedThrowable)current).getOriginalErrorClassName();
                    } else {
                        className = current.getClass().getName();
                    }
                    System.out.println("className = " + className);
                    if (className.endsWith("MappingException")) {
                        GraphVisitorCustomTarget.ResultCollector.remove(resultSetId);
                        return; // e was an instance of MappingException, so test passed
                    }
                    current = current.getCause();
                } while (current != null);
                GraphVisitorCustomTarget.ResultCollector.remove(resultSetId);
                fail("Negative test should've failed with a MappingException!");
            }
        }

        List<String> rdfStrings = GraphVisitorCustomTarget.ResultCollector.remove(resultSetId);

        if(!positive) {
            if (rdfStrings.isEmpty()) {
                // OK
                return;
            } else {
                System.err.println("Error or empty result expected. Was:\n" + rdfStrings);
                fail("There should have been an empty result or an exception thrown for negative testcase");
            }
        }

        StringBuilder result = new StringBuilder();
        for (String rdf : rdfStrings) {
            String s = rdf + "\n";
            result.append(s);
        }

        String actualQuads = result.toString();

        Path expectedOutputPath;
        Path pathNQ = Paths.get(basePath, directory, "output.nq");
        Path pathTTL = Paths.get(basePath, directory, "output.ttl");
        Path defaultNQ = Paths.get(basePath, directory, "default.nq");
        if (Files.exists(pathNQ)) {
            expectedOutputPath = pathNQ;
        } else if (Files.exists(pathTTL)) {
            expectedOutputPath = pathTTL;
        } else if (Files.exists(defaultNQ)) {
            expectedOutputPath = defaultNQ;
        } else {
            throw new IllegalArgumentException("Expected output path does not exist!");
        }

        List<Quad> expected = new ArrayList<>();
        TestCore.getDatasetGraph(new File(expectedOutputPath.toString())).find().forEachRemaining(expected::add);
        List<Quad> actual = new ArrayList<>();
        TestCore.getDatasetGraph(actualQuads).find().forEachRemaining(actual::add);

        // move any quads that are on the RR:defaultGraph into Jena's default graph
        actual = actual.stream().map(q -> {
            if (q.getGraph().getURI().equals(NAMESPACES.RR + "defaultGraph")) {
                return new Quad(Quad.defaultGraphIRI, q.getSubject(), q.getPredicate(), q.getObject());
            }
            return q;
        }).toList();

        Assertions.assertEquals(expected, actual, String.format("Failed test case: %s \n", directory));
    }
}
