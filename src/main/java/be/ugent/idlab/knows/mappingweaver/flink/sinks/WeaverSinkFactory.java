package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
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

    public WeaverSinkFactory(JSONObject config, String operatorName, String targetVariable) {
        String targetType = config.getString("target_type");
        this.targetType = WeaverSinkFactory.TargetType.valueOf(targetType);
        this.config = config;
        this.operatorName = operatorName;
        this.targetVariable = targetVariable;
    }

    public void attachSink(DataStream<MapTupValue> dataStream) {
        DataStream<String> stringStream = dataStream.map(new SolMapValueToStringExtractor(this.targetVariable));
        switch (this.targetType) {
            case StdOut -> {
                stringStream.sinkTo(new STDSink()).name(this.operatorName);
            }
            case Kafka -> {
                throw new UnsupportedOperationException("Kafka target is not supported yet!");
            }
            case File -> {
                throw new UnsupportedOperationException("File target is not supported yet!");
            }
            case WebSocket -> {
                throw new UnsupportedOperationException("Websocket target is not supported yet!");
            }
        }
    }

}
