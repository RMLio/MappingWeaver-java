package be.ugent.idlab.knows.mappingweaver.flink.source.dataio;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import be.ugent.idlab.knows.amo.operators.source.SourceOperator;

public class FlinkDataIOEnumerator implements SplitEnumerator<FlinkDataIOSplit, FlinkSplitEnumeratorCheckpoint> {

    private final SplitEnumeratorContext<FlinkDataIOSplit> context;
    private Optional<SourceOperator> sourceOperatorOpt;

    public FlinkDataIOEnumerator(SplitEnumeratorContext<FlinkDataIOSplit> context,
            SourceOperator sourceOperator) {
        this.context = context;
        this.sourceOperatorOpt = Optional.of(sourceOperator);
    }

    // Implement this if you want a push-based reader
    @Override
    public void addReader(int subtaskId) {
    }

    @Override
    public void addSplitsBack(List<FlinkDataIOSplit> splits, int subtaskId) {
        FlinkDataIOSplit split = splits.getFirst();
        this.sourceOperatorOpt = Optional.of(split.getSourceOperator());

    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void handleSplitRequest(int subtaskId, String hostName) {
        if (!this.sourceOperatorOpt.isEmpty()) {
            
            this.context.assignSplit(new FlinkDataIOSplit(this.sourceOperatorOpt.get()), subtaskId);
            this.sourceOperatorOpt = Optional.empty();
        }else{
            this.context.signalNoMoreSplits(subtaskId); 

        }
    }

    @Override
    public FlinkSplitEnumeratorCheckpoint snapshotState(long checkpointId) throws Exception {
        return new FlinkSplitEnumeratorCheckpoint(this.sourceOperatorOpt.get());
    }

    @Override
    public void start() {
    }

}
