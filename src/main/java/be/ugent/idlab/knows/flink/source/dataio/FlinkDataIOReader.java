package be.ugent.idlab.knows.flink.source.dataio;

import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.exceptions.MappingException;
import be.ugent.idlab.knows.values.MapTupValue;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Deprecated
public class FlinkDataIOReader implements SourceReader<MapTupValue, FlinkDataIOSplit> {
    private boolean isNoMoreSplit;
    private boolean currrentSplitInitialized;
    private final SourceReaderContext context;
    private Optional<FlinkDataIOSplit> currrentSplit;
    private final ReentrantReadWriteLock lock;

    public FlinkDataIOReader(SourceReaderContext context) {
        this.context = context;
        this.currrentSplit = Optional.empty();
        this.currrentSplitInitialized = false;
        this.isNoMoreSplit = false;
        this.lock = new ReentrantReadWriteLock();
    }

    @Override
    public void close() throws Exception {
        // TODO: Check ways to properly close DataIO sources <21-08-24, Min Oo> //
    }

    @Override
    public void addSplits(List<FlinkDataIOSplit> splits) {

        // TODO: Expand this to also handle multiple splits <21-08-24, Min Oo> //
        lock.writeLock().lock();
        if (!splits.isEmpty()) {
            FlinkDataIOSplit split = splits.getFirst();
            this.currrentSplit = Optional.of(split);
        }
        lock.writeLock().unlock();

    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        CompletableFuture<Void> future = CompletableFuture.supplyAsync(() -> {
            try {
                this.lock.writeLock().lock();
                if (this.currrentSplit.isPresent() && !this.currrentSplitInitialized) {
                    FlinkDataIOSplit split = this.currrentSplit.get();
                    SourceOperator operator = split.getSourceOperator();
                    try {
                        operator.init();
                        this.currrentSplitInitialized = true;
                    } catch (SQLException psqlException) {
                        throw new MappingException(psqlException.getMessage());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
            return null;
        });

        return future;
    }

    @Override
    public void notifyNoMoreSplits() {
        this.lock.writeLock().lock();
        this.isNoMoreSplit = true;
        this.lock.writeLock().unlock();
    }

    @Override
    public InputStatus pollNext(ReaderOutput<MapTupValue> collector) {
        if (this.currrentSplit.isEmpty()) {
            if (!this.isNoMoreSplit) {
                this.context.sendSplitRequest();
                return InputStatus.NOTHING_AVAILABLE;
            } else {

                return InputStatus.END_OF_INPUT;
            }
        }
        if (!this.currrentSplitInitialized) {

            return InputStatus.NOTHING_AVAILABLE;
        }

        SourceOperator operator = this.currrentSplit.get().getSourceOperator();

        if (operator.hasNext() && this.currrentSplitInitialized) {

            this.lock.readLock().lock();
            MapTupValue record = new MapTupValue(operator.next());
            collector.collect(record);
            this.lock.readLock().unlock();
        }

        if (operator.hasNext()) {
            return InputStatus.MORE_AVAILABLE;
        }

        return InputStatus.END_OF_INPUT;

    }

    @Override
    public List<FlinkDataIOSplit> snapshotState(long arg0) {
        return this.currrentSplit.map(Arrays::asList)
                .orElseGet(ArrayList::new);
    }

    @Override
    public void start() {
    }
}
