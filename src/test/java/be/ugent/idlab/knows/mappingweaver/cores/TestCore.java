package be.ugent.idlab.knows.mappingweaver.cores;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import be.ugent.idlab.knows.mappingweaver.utilities.GraphVisitorCustomTarget;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.SerializedThrowable;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.FactoryRDFStd;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.IsoMatcher;
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
import java.util.stream.Collectors;

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


    /**
     * Runs the {@code mapping.ttl} of a case folder and compares its output with the folder's {@code output.nq} as RDF
     * datasets, by isomorphism: graphs matched by name, one blank node mapping across all graphs, order and duplicates
     * ignored. This is how the conformance runner judges a case (HANDBOOK: Conformance).
     *
     * @param caseDir the case folder, also the folder relative paths of the mapping start from
     */
    public void isomorphicTest(Path caseDir) throws Exception {
        String plan = ITranslator.getInstance().translate_to_document(Files.readString(caseDir.resolve("mapping.ttl")));
        String actual = String.join("\n", execute(plan, caseDir.toString(), false));
        DatasetGraph expectedGraph = RDFParser.source(caseDir.resolve("output.nq")).lang(Lang.NQ).toDatasetGraph();
        DatasetGraph actualGraph = RDFParser.fromString(actual, Lang.NQ).toDatasetGraph();
        if (!IsoMatcher.isomorphic(expectedGraph, actualGraph)) {
            fail(String.format("Output of %s differs from output.nq.%nExpected:%n%s%nActual:%n%s", caseDir,
                    sortedNQuads(expectedGraph), sortedNQuads(actualGraph)));
        }
    }

    private static String sortedNQuads(DatasetGraph graph) {
        StringWriter writer = new StringWriter();
        RDFDataMgr.write(writer, graph, RDFFormat.NQUADS);
        return writer.toString().lines().sorted().collect(Collectors.joining("\n"));
    }

    /**
     * Executes a mapping plan on the shared Flink mini-cluster.
     *
     * @return the generated quads, as N-Quads lines
     */
    private static List<String> execute(String mappingPlan, String directory, boolean bestEffort) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        String resultSetId = UUID.randomUUID().toString();
        GraphVisitorCustomTarget.ResultCollector.initialize(resultSetId);
        List<String> rdfStrings;
        try {
            MappingPlan plan = MappingPlan.fromString(env, mappingPlan, directory, "http://example.com/", bestEffort);
            GraphOpVisitor visitor = new GraphVisitorCustomTarget(env, plan.getOperatorGraph(),
                    TargetOperator.TARGET_VARIABLE, resultSetId);
            plan.setVisitor(visitor);

            plan.initializeFlinkTopology(true);
            plan.execute();
        } finally {
            rdfStrings = GraphVisitorCustomTarget.ResultCollector.remove(resultSetId);
        }
        return rdfStrings;
    }

    private void runTestWithMappingPlan(String basePath, String directory, String mappingPlan, boolean positive, boolean bestEffort) throws FileNotFoundException {
        System.out.printf("Testing test case: %s%n", directory);
        List<String> rdfStrings;
        try {
            rdfStrings = execute(mappingPlan, Paths.get(basePath, directory).toString(), bestEffort);
        } catch (Throwable e) {
            if (positive) { // if positive, rethrow the exception to cause the test to fail
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
                        return; // e was an instance of MappingException, so test passed
                    }
                    current = current.getCause();
                } while (current != null);
                fail("Negative test should've failed with a MappingException!");
                return;
            }
        }


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
        if (Files.exists(pathNQ)) {
            expectedOutputPath = pathNQ;
        } else if (Files.exists(pathTTL)) {
            expectedOutputPath = pathTTL;
        } else {
            throw new IllegalArgumentException("Expected output path does not exist!");
        }

        List<Quad> expected = new ArrayList<>();
        TestCore.getDatasetGraph(new File(expectedOutputPath.toString())).find().forEachRemaining(expected::add);
        List<Quad> actual = new ArrayList<>();
        TestCore.getDatasetGraph(actualQuads).find().forEachRemaining(actual::add);

        Assertions.assertEquals(expected, actual, String.format("Failed test case: %s \n", directory));
    }
}
