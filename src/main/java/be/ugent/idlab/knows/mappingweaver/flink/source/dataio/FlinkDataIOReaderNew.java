package be.ugent.idlab.knows.mappingweaver.flink.source.dataio;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

public class FlinkDataIOReaderNew implements SourceReader<MapTupValue, FlinkDataIOSplit> {
    private boolean isNoMoreSplit;
    private final SourceReaderContext context;
    private final Queue<FlinkDataIOSplit> splits;

    public FlinkDataIOReaderNew(SourceReaderContext context) {
        this.context = context;
        this.splits = new ArrayDeque<>();
        this.isNoMoreSplit = false;
    }

    @Override
    public void start() {
    }

    @Override
    public InputStatus pollNext(ReaderOutput<MapTupValue> output) throws Exception {
        if (this.splits.isEmpty()) {
            if (this.isNoMoreSplit) {
                return InputStatus.END_OF_INPUT;
            } else {
                this.context.sendSplitRequest();
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        FlinkDataIOSplit head = this.splits.peek();
        SourceOperator operator = head.getSourceOperator();

        while (!operator.hasNext()) {
            if (!operator.isReady()) {
                try {
                    operator.init();
                } catch (SQLException sql) {
                    throw new MappingException(sql.getMessage());
                }
            } else {
                this.splits.poll();
                if (this.splits.isEmpty()) {
                    this.context.sendSplitRequest();
                    return InputStatus.NOTHING_AVAILABLE;
                }

                head = this.splits.peek();
                operator = head.getSourceOperator();
            }
        }

        // operator now certainly has something
        try {
            MappingTuple tuple = operator.next();
            output.collect(new MapTupValue(tuple));
        } catch (Exception ex) {
            throw new MappingException(ex);
        }

        if (operator.hasNext()) {
            return InputStatus.MORE_AVAILABLE;
        }

        // a shortcut to cause Flink to poll again, to re-execute the while loop above
        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<FlinkDataIOSplit> snapshotState(long checkpointId) {
        return this.splits.stream().toList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // TODO: make this actually check if there's something available
        return CompletableFuture.supplyAsync(() -> null);
    }

    @Override
    public void addSplits(List<FlinkDataIOSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void notifyNoMoreSplits() {
        this.isNoMoreSplit = true;
    }

    @Override
    public void close() throws Exception {

    }
}
