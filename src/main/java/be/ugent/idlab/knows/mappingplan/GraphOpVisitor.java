package be.ugent.idlab.knows.mappingplan;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.OperatorVisitor;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.BinaryOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.BinaryType;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.JoinOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.UnaryOperator;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.DataIOSourceOperator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.dataio.access.VirtualAccess;
import be.ugent.idlab.knows.flink.operators.FlinkJoinOperator;
import be.ugent.idlab.knows.flink.source.KafkaSourceOperator;
import be.ugent.idlab.knows.flink.source.dataio.FlinkDataIOBoundedSource;
import be.ugent.idlab.knows.mappingplan.OperatorGraph.FragmentOperatorPair;
import be.ugent.idlab.knows.values.MapTupValue;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A visitor connecting the operators together into a chain
 */
public class GraphOpVisitor implements OperatorVisitor<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GraphOpVisitor.class);
    protected final StreamExecutionEnvironment env;
    protected final OperatorGraph operatorGraph;
    protected final Map<Operator, DataStream<MapTupValue>> streamCache = new HashMap<>();
    protected final Map<Operator, DataStreamSink<MapTupValue>> sinks = new HashMap<>();
    protected Long watermarkInterval = null;
    protected Boolean isLocalParallel = null;

    public GraphOpVisitor(StreamExecutionEnvironment env, OperatorGraph graph) {
        this.env = env;
        this.operatorGraph = graph;
    }

    public GraphOpVisitor(StreamExecutionEnvironment env, OperatorGraph graph, long watermarkInterval) {
        this(env, graph);
        this.watermarkInterval = watermarkInterval;
    }

    public void setWatermarkInterval(Long watermarkInterval) {
        this.watermarkInterval = watermarkInterval;
    }

    public void setLocalParallel(boolean isLocalParallel) {
        this.isLocalParallel = isLocalParallel;
    }

    @Deprecated
    private Void oldVisitSource(SourceOperator sourceOperator) {
        MappingTuple tuple = sourceOperator.consumeSource();
        DataStream<MapTupValue> stream = this.env.fromData(new MapTupValue(tuple));
        this.streamCache.put(sourceOperator, stream);
        return null;
    }

    @Override
    public Void visitSource(@NonNull SourceOperator sourceOperator) {

        if (sourceOperator instanceof KafkaSourceOperator kafkaOperator) {
            // build the infrastructure for consuming Kafka
            KafkaSource<String> source = KafkaSource.<String>builder()
                    .setBootstrapServers(kafkaOperator.getBrokers())
                    .setGroupId(kafkaOperator.getGroupId())
                    .setTopics(kafkaOperator.getTopic())
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .build();

            // create a stream from Kafka
            DataStream<MapTupValue> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "KafkaSource")
                    // consume all records using the underlying reference formulation and send them into the pipeline
                    .flatMap(new StringToMapTupValueFlatMap(kafkaOperator.getUnderlyingOperator()));

            if (this.isLocalParallel != null) {
                stream = stream.keyBy(e -> System.nanoTime());
            }

            if (this.watermarkInterval != null) {
                stream = stream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(this.watermarkInterval)));
            }

            this.streamCache.put(sourceOperator, stream);
            return null;
        } else {
            FlinkDataIOBoundedSource source = new FlinkDataIOBoundedSource(sourceOperator);
            DataStream<MapTupValue> stream = this.env.fromSource(source, WatermarkStrategy.noWatermarks(),
                    sourceOperator.getOperatorName());
            this.streamCache.put(sourceOperator, stream);
        }


        return null;
    }

    @Override
    public Void visitUnary(@NonNull UnaryOperator unaryOperator) {
        List<FragmentOperatorPair> parents = this.operatorGraph.getParents(unaryOperator);
        Operator parent = parents.getFirst().operator();
        DataStream<MapTupValue> parentStream = this.streamCache.get(parent);
        DataStream<MapTupValue> childStream = parentStream.map(new UnaryMappingFunction(unaryOperator)).name(unaryOperator.getOperatorName());
        this.streamCache.put(unaryOperator, childStream);

        return null;
    }

    @Override
    public Void visitBinary(@NonNull BinaryOperator binaryOperator) {
        List<FragmentOperatorPair> parents = this.operatorGraph.getParents(binaryOperator);

        FragmentOperatorPair firstPair = parents.get(0);
        FragmentOperatorPair secondPair = parents.get(1);
        DataStream<MapTupValue> leftStream, rightStream;
        if (firstPair.fragment().direction().equals("Left")) {
            leftStream = this.streamCache.get(firstPair.operator());
            rightStream = this.streamCache.get(secondPair.operator());
        } else {
            rightStream = this.streamCache.get(firstPair.operator());
            leftStream = this.streamCache.get(secondPair.operator());
        }

        ConnectedStreams<MapTupValue, MapTupValue> connected = leftStream.connect(rightStream);

        BinaryType binaryType = binaryOperator.getBinaryOpType();
        if (binaryType.equals(new BinaryType.LeftJoin()) || binaryType.equals(new BinaryType.ThetaJoin())
                || binaryType.equals(new BinaryType.NaturalJoin())) {

            LOGGER.warn(String.format("Binary type! %s", binaryType));
            // Set parallelism to 1 for join operators for now
            // FIXME: Make sure to use as much parallelism when joining as possible!
            // <09-09-24, Sitt Min Oo> //
            DataStream<MapTupValue> childStream = connected
                    .flatMap(new FlinkJoinOperator((JoinOperator) binaryOperator))
                    .name(binaryOperator.getOperatorName())
                    .setParallelism(1);
            this.streamCache.put(binaryOperator, childStream);
        }

        return null;
    }

    @Override
    public Void visitTarget(@NonNull TargetOperator targetOperator) {
        List<FragmentOperatorPair> parents = this.operatorGraph.getParents(targetOperator);
        Operator parent = parents.getFirst().operator();
        DataStream<MapTupValue> parentStream = this.streamCache.get(parent);
        parentStream.addSink(new TargetSinkFunction(targetOperator)).name(targetOperator.getOperatorName());

        return null;
    }

    public Map<Operator, DataStream<MapTupValue>> getStreamCache() {
        return streamCache;
    }

    static class UnaryMappingFunction implements MapFunction<MapTupValue, MapTupValue> {

        private final UnaryOperator operator;

        public UnaryMappingFunction(UnaryOperator operator) {
            this.operator = operator;
        }

        @Override
        public MapTupValue map(MapTupValue value) {
            return new MapTupValue(this.operator.apply(value.getValue()));
        }
    }

    protected static class StringToMapTupValueFlatMap implements FlatMapFunction<String, MapTupValue> {

        private final DataIOSourceOperator dataIOSourceOperator;

        public StringToMapTupValueFlatMap(DataIOSourceOperator underlyingOperator) {
            this.dataIOSourceOperator = underlyingOperator;
        }

        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and
         * transforms it into zero, one, or more elements.
         *
         * @param value     The input value.
         * @param collector The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the
         *                   operation to fail and may trigger recovery.
         */
        @Override
        public void flatMap(String value, Collector<MapTupValue> collector) throws Exception {
            VirtualAccess access = new VirtualAccess(value.getBytes(Charset.defaultCharset()));

            this.dataIOSourceOperator.setAccess(access);

            this.dataIOSourceOperator.init();

            // collect all records
            while (this.dataIOSourceOperator.hasNext()) {
                collector.collect(new MapTupValue(this.dataIOSourceOperator.next()));
            }
        }
    }
}
