package be.ugent.idlab.knows.mappingweaver.flink.source.dataio;

import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.mappingweaver.flink.source.serializers.FlinkDataIOSplitSerializer;
import be.ugent.idlab.knows.mappingweaver.flink.source.serializers.FlinkSplitEnumCheckpointSerializer;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class FlinkDataIOBoundedSource
        implements Source<MapTupValue, FlinkDataIOSplit, FlinkSplitEnumeratorCheckpoint> {

    private final SourceOperator sourceOperator;

    public FlinkDataIOBoundedSource(SourceOperator sourceOperator) {
        this.sourceOperator = sourceOperator;
    }

    @Override
    public SourceReader<MapTupValue, FlinkDataIOSplit> createReader(SourceReaderContext context) throws Exception {
        return new FlinkDataIOReaderNew(context);
    }

    @Override
    public SplitEnumerator<FlinkDataIOSplit, FlinkSplitEnumeratorCheckpoint> createEnumerator(
            SplitEnumeratorContext<FlinkDataIOSplit> context) throws Exception {

        return new FlinkDataIOEnumerator(context, this.sourceOperator);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SimpleVersionedSerializer<FlinkSplitEnumeratorCheckpoint> getEnumeratorCheckpointSerializer() {
        return new FlinkSplitEnumCheckpointSerializer();
    }

    @Override
    public SimpleVersionedSerializer<FlinkDataIOSplit> getSplitSerializer() {
        return new FlinkDataIOSplitSerializer();
    }

    @Override
    public SplitEnumerator<FlinkDataIOSplit, FlinkSplitEnumeratorCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FlinkDataIOSplit> context, FlinkSplitEnumeratorCheckpoint checkpoint)
            throws Exception {

        SourceOperator operator = checkpoint.getSourceOperator();
        return new FlinkDataIOEnumerator(context, operator);

    }

}
