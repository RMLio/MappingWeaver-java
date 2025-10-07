package be.ugent.idlab.knows.mappingweaver.cores;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.dataio.utils.NAMESPACES;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.utilities.GraphVisitorCustomTarget;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParser;
import org.apache.jena.riot.lang.LabelToNode;
import org.apache.jena.riot.system.FactoryRDFStd;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.junit.jupiter.api.Assertions;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.fail;

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

    public void positiveTest(String basePath, String directory) throws Exception {
        this.runTest(basePath, directory, true);
    }

    public void positiveTest(String basePath, String directory, String mappingPlan) throws FileNotFoundException {
        this.runTestWithMappingPlan(basePath, directory, mappingPlan, true);
    }

    public void positiveTestTurtlePlan(String basePath, String directory, String turtlePlan) throws FileNotFoundException {
        String mappingPlan = ITranslator.getInstance().translate_to_document(turtlePlan);

        this.runTestWithMappingPlan(basePath, directory, mappingPlan, true);
    }

    public void negativeTest(String basePath, String directory) throws Exception {
        this.runTest(basePath, directory, false);
    }

    public void negativeTestTurtlePlan(String basePath, String directory, String turtlePlan) throws FileNotFoundException {
        String mappingPlan = ITranslator.getInstance().translate_to_document(turtlePlan);

        this.runTestWithMappingPlan(basePath, directory, mappingPlan, false);
    }

    private void runTest(String basePath, String directory, boolean positive) throws IOException {
        try {
            String plan = this.getMappingPlan(basePath, directory);
            runTestWithMappingPlan(basePath, directory, plan, positive);
        } catch (Throwable t) {
            if (positive) {
                throw t;
            }
        }
    }

    private String getMappingPlan(String basePath, String directory) throws IOException {
//        String mappingPath = directory + "/mapping.ttl";
        String mapping = Files.readString(Paths.get(basePath, directory, "mapping.ttl"));
        ITranslator translator = ITranslator.getInstance();

        return translator.translate_to_document(mapping);
    }

    private void runTestWithMappingPlan(String basePath, String directory, String mappingPlan, boolean positive) throws FileNotFoundException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        GraphVisitorCustomTarget.ResultCollector.values.clear();
//        System.out.println(mappingPlan);

        System.out.printf("Testing test case: %s%n", directory);
        try {
            MappingPlan plan = MappingPlan.fromString(env, mappingPlan, Paths.get(basePath, directory).toString());
//            System.out.println(plan.getOperatorGraph().getOperators());
            GraphOpVisitor visitor = new GraphVisitorCustomTarget(env, plan.getOperatorGraph(),
                    TargetOperator.TARGET_VARIABLE);
            plan.setVisitor(visitor);

            plan.execute();
        } catch (Throwable e) {
            if (positive) { // if positive, rethrow the exception to cause the test to fail
                throw new RuntimeException(e);
            } else {
                // scan the exception stack for a MappingException
                Throwable cause = e;
                while (!(cause instanceof MappingException)) {
                    if (cause.getCause() == null) {
                        fail("Negative tes<t should've failed with a MappingException!");
                    } else {
                        cause = cause.getCause();
                    }
                }

                return; // cause was an instance of MappingException, so test passed
            }
        }

        if(!positive) {
            fail("There should have been an exception thrown for negative testcase");
        }

        StringBuilder result = new StringBuilder();
        for (String rdf : GraphVisitorCustomTarget.ResultCollector.values) {
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
