package be.ugent.idlab.knows.utilities;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.flink.source.KafkaSourceOperator;
import be.ugent.idlab.knows.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.mappingplan.OperatorGraph.FragmentOperatorPair;
import be.ugent.idlab.knows.values.MapTupValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Class mocking the GraphOpVisitor with a custom target
 */
public class GraphVisitorCustomTarget extends GraphOpVisitor {

    private final String targetVariable;

    public GraphVisitorCustomTarget(StreamExecutionEnvironment env, OperatorGraph graph, String targetVariable) {
        super(env, graph);
        this.targetVariable = targetVariable;
    }

    @Override
    public Void visitTarget(@NonNull TargetOperator targetOperator) {
        List<FragmentOperatorPair> parents = this.operatorGraph.getParents(targetOperator);
        Operator parent = parents.getFirst().operator();
        DataStream<MapTupValue> parentStream = this.streamCache.get(parent);
        parentStream.addSink(new ResultCollector(targetOperator.getInputFragments(), this.targetVariable));

        return null;
    }

    @Override
    public Void visitSource(@NonNull SourceOperator sourceOperator) {
        if (sourceOperator instanceof KafkaSourceOperator kafkaOperator) {
            // TODO: this code basically copies the GraphOpVisitor code, except it adds a boundedness condition
            //  consider refactoring
            // build the infrastructure for consuming Kafka
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaOperator.getBrokers())
                    .setGroupId(kafkaOperator.getGroupId())
                    .setTopics(kafkaOperator.getTopic())
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    // artificially create a bounded stream
                    .setBounded(OffsetsInitializer.timestamp(System.currentTimeMillis()))
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // create a stream from Kafka
            DataStream<MapTupValue> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")
                    // consume all records using the underlying reference formulation and send them into the pipeline
                    .flatMap(new StringToMapTupValueFlatMap(kafkaOperator.getUnderlyingOperator()));
            this.streamCache.put(sourceOperator, stream);
            return null;

        } else {
            return super.visitSource(sourceOperator);
        }
    }

    public static class ResultCollector implements SinkFunction<MapTupValue> {
        public static final List<String> values = Collections.synchronizedList(new ArrayList<>());

        private final Set<String> targetFragments;
        private final String targetVariable;

        public ResultCollector(Set<String> targetFragments, String targetVariable) {
            this.targetFragments = targetFragments;
            this.targetVariable = targetVariable;
        }

        @Override
        public void invoke(MapTupValue value, Context context) {
            MappingTuple mappingTuple = value.getValue();
            if (mappingTuple != null) {
                for (String targetFragment : targetFragments) {
                    mappingTuple.getSolutionMappings(targetFragment).forEach(sm -> {
                        if (sm != null) {
                            RDFNode solution = sm.get(targetVariable);
                            if (solution != null && !solution.isNull() && solution instanceof LiteralNode) {
                                values.add(solution.getValue().toString());
                            }
                        }
                    });
                }
            }
        }
    }


}
