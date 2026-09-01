package be.ugent.idlab.knows.mappingweaver.fno;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno.FnOFunction;
import be.ugent.idlab.knows.mappingweaver.utilities.GraphVisitorCustomTarget;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExternalFunctionTests {
    private static final String DIR = "src/test/resources/custom/fno/external-function-test";

    @Test
    public void aFunctionFromAGivenDescriptionIsUsed() throws Exception {
        // the description names its jar relative to the working directory
        Path description = Paths.get(DIR, "myfunctions.ttl").toAbsolutePath();

        String plan = ITranslator.getInstance()
                .translate_to_document(Files.readString(Paths.get(DIR, "mapping.ttl")));

        FnOFunction.configure(List.of(description.toString()), false);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        GraphVisitorCustomTarget.ResultCollector.values.clear();

        MappingPlan mappingPlan = MappingPlan.fromString(env, plan, DIR, "http://example.com/",false);
        mappingPlan.setVisitor(new GraphVisitorCustomTarget(env, mappingPlan.getOperatorGraph(),
                TargetOperator.TARGET_VARIABLE));
        mappingPlan.initializeFlinkTopology(true);
        mappingPlan.execute();

        assertEquals(List.of("<http://example.com/urn:example:alice> <urn:example:shouted> \"ALICE!\" ."),
                GraphVisitorCustomTarget.ResultCollector.values);
    }

}
