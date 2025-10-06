package be.ugent.idlab.knows.flink.sinks;

import be.ugent.idlab.knows.amo.functions.TargetSink;
import org.jspecify.annotations.Nullable;

public class KafkaSink implements TargetSink<String> {

    public KafkaSink(String broker, String topic) {

    }
    @Override
    public void sink(@Nullable String serializedOutput) {

    }
}
