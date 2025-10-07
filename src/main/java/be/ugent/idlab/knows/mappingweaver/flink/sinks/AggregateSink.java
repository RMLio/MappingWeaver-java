package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import be.ugent.idlab.knows.amo.functions.TargetSink;

import java.io.IOException;

public interface AggregateSink<T> extends TargetSink<T> {
    /**
     * Method responsible for writing all remaining data to the sink and closing the resource
     */
    void writeback() throws IOException;
}
