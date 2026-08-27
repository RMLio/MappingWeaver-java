package be.ugent.idlab.knows.mappingweaver.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jspecify.annotations.NonNull;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingweaver.flink.source.KafkaSourceOperator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingweaver.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.mappingweaver.mappingplan.OperatorGraph.FragmentOperatorPair;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;

/**
 * Class mocking the GraphOpVisitor with a custom target
 */
public class GraphVisitorCustomTarget extends GraphOpVisitor {

    private final String targetVariable;
    private final String resultSetId;

    public GraphVisitorCustomTarget(StreamExecutionEnvironment env, OperatorGraph graph, String targetVariable) {
        this(env, graph, targetVariable, ResultCollector.DEFAULT_RESULT_SET);
    }

    public GraphVisitorCustomTarget(StreamExecutionEnvironment env, OperatorGraph graph, String targetVariable,
                                    String resultSetId) {
        super(env, graph);
        this.targetVariable = targetVariable;
        this.resultSetId = resultSetId;
    }

    @Override
    public Void visitTarget(@NonNull TargetOperator targetOperator) {
        List<FragmentOperatorPair> parents = this.operatorGraph.getParents(targetOperator);
        Operator parent = parents.getFirst().operator();
        DataStream<MapTupValue> parentStream = this.streamCache.get(parent);
        parentStream.sinkTo(new ResultCollector(targetOperator.getInputFragments(), this.targetVariable,
            this.resultSetId));

        return null;
    }

    @Override
    public Void visitSource(@NonNull SourceOperator sourceOperator) {
        if (sourceOperator instanceof KafkaSourceOperator kafkaOperator) {
            // TODO: this code basically copies the GraphOpVisitor code, except it adds a
            // boundedness condition
            // consider refactoring
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
                    // consume all records using the underlying reference formulation and send them
                    // into the pipeline
                    .flatMap(new StringToMapTupValueFlatMap(kafkaOperator.getUnderlyingOperator()));
            this.streamCache.put(sourceOperator, stream);
            return null;

        } else {
            return super.visitSource(sourceOperator);
        }
    }

    public static class ResultCollector implements Sink<MapTupValue> {
        private static final String DEFAULT_RESULT_SET = "default";
        private static final Map<String, List<String>> RESULT_SETS = new ConcurrentHashMap<>();
        public static final List<String> values = resultSet(DEFAULT_RESULT_SET);

        private final Set<String> targetFragments;
        private final String targetVariable;
        private final String resultSetId;

        public ResultCollector(Set<String> targetFragments, String targetVariable) {
            this(targetFragments, targetVariable, DEFAULT_RESULT_SET);
        }

        public ResultCollector(Set<String> targetFragments, String targetVariable, String resultSetId) {
            this.targetFragments = targetFragments;
            this.targetVariable = targetVariable;
            this.resultSetId = resultSetId;
        }

        public static void initialize(String resultSetId) {
            RESULT_SETS.put(resultSetId, Collections.synchronizedList(new ArrayList<>()));
        }

        public static List<String> remove(String resultSetId) {
            List<String> result = RESULT_SETS.remove(resultSetId);
            return result == null ? List.of() : List.copyOf(result);
        }

        private static List<String> resultSet(String resultSetId) {
            return RESULT_SETS.computeIfAbsent(resultSetId,
                    ignored -> Collections.synchronizedList(new ArrayList<>()));
        }

        @Override
        public SinkWriter<MapTupValue> createWriter(WriterInitContext arg0) throws IOException {
            return new ResultSinkWriter();
        }

        public class ResultSinkWriter implements SinkWriter<MapTupValue> {

            @Override
            public void close() throws Exception {
            }

            @Override
            public void flush(boolean arg0) throws IOException, InterruptedException {
            }

            @Override
            public void write(MapTupValue value, Context arg1) throws IOException, InterruptedException {
                MappingTuple mappingTuple = value.getValue();
                if (mappingTuple != null) {
                    for (String targetFragment : targetFragments) {
                        mappingTuple.getSolutionMappings(targetFragment).forEach(sm -> {
                            if (sm != null) {
                                RDFNode solution = sm.get(targetVariable);
                                if (solution != null && !solution.isNull() && solution instanceof LiteralNode) {
                                    resultSet(resultSetId).add(solution.getValue().toString());
                                }
                            }
                        });
                    }
                }

            }

        }

    }

}
