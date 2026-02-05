package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import be.ugent.idlab.knows.amo.functions.TargetSink;
import org.jspecify.annotations.Nullable;

@Deprecated
public class KafkaSink implements TargetSink<String> {

    public KafkaSink(String broker, String topic) {

    }
    @Override
    public void sink(@Nullable String serializedOutput) {

    }
}
