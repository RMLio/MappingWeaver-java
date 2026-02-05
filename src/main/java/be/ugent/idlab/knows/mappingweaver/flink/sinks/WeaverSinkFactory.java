package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.json.JSONObject;

import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;

public class WeaverSinkFactory {
    public enum TargetType {
        StdOut,
        Kafka,
        File,
        WebSocket,
    }

    private TargetType targetType;
    private JSONObject config;
    private String operatorName;
    private String targetVariable;

    public WeaverSinkFactory(TargetType targetType, String operatorName, String targetVariable, JSONObject config) {
        this.targetType = targetType;
        this.operatorName = operatorName;
        this.targetVariable = targetVariable;
        this.config = config;
    }

    public WeaverSinkFactory(JSONObject config, String operatorName, String targetVariable) {
        String targetType = config.getString("target_type");
        new WeaverSinkFactory(WeaverSinkFactory.TargetType.valueOf(targetType), operatorName, targetVariable, config);
    }

    public void attachSink(DataStream<MapTupValue> dataStream) {
        DataStream<String> stringStream = dataStream.map(new SolMapValueToStringExtractor(this.targetVariable));
        DataStreamSink<String> sunkStream = switch (this.targetType) {
            case StdOut -> {
                yield stringStream.sinkTo(new STDSink());
            }
            case Kafka -> {
                throw new UnsupportedOperationException("Kafka target is not supported yet!");
            }
            case File -> {
                String outputPath = this.config.getString("path");
                FileSink<String> sink = FileSink.forRowFormat(new Path(outputPath),
                        new SimpleStringEncoder<String>("UTF-8"))
                        .withRollingPolicy(DefaultRollingPolicy.builder()
                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
                                .build())
                        .build();

                yield stringStream.sinkTo(sink).setParallelism(1);
            }
            case WebSocket -> {
                throw new UnsupportedOperationException("Websocket target is not supported yet!");
            }
        };

        sunkStream.name(this.operatorName);
    }

}
