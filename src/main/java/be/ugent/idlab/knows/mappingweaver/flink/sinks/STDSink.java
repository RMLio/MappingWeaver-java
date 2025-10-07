package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.TargetSink;
import org.jspecify.annotations.Nullable;

/**
 * A TargetSink that prints out the output to the standard output.
 */
public class STDSink implements TargetSink<String> {
    @Override
    public void sink(@Nullable String data) {
        System.out.println(data);
    }
}

