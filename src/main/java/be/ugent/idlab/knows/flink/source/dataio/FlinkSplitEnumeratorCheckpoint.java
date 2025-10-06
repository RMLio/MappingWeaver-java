package be.ugent.idlab.knows.flink.source.dataio;

import java.io.Serializable;

import javax.annotation.Nullable;

import be.ugent.idlab.knows.amo.operators.source.SourceOperator;

public class FlinkSplitEnumeratorCheckpoint implements Serializable {

    private SourceOperator sourceOperator;

    public FlinkSplitEnumeratorCheckpoint(@Nullable SourceOperator sourceOperator) {
        this.sourceOperator = sourceOperator;
    }

    public SourceOperator getSourceOperator() {
        return sourceOperator;
    }

}
