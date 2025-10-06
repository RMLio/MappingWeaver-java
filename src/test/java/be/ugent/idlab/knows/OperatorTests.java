package be.ugent.idlab.knows;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A class showcasing the usage of each and every operator in a pipeline,
 * verifying its functionality underneath Flink
 */
public class OperatorTests {

    // Flink Minicluster that will be reused across the tests
    @ClassRule
    public static MiniClusterResource flinkCluster = new MiniClusterResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberSlotsPerTaskManager(2)
                    .setNumberTaskManagers(1)
                    .build());

    @Test
    public void testExtend() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        Collector.values.clear();
        MappingTuple in = new MappingTuple();
        in.setSolutionMaps("default", new SolutionMapping(Map.of("foo", new LiteralNode("bar"))));


    }

    private static class Collector implements SinkFunction<RDFNode> {

        public static final List<RDFNode> values = Collections.synchronizedList(new ArrayList<>());
    }
}
