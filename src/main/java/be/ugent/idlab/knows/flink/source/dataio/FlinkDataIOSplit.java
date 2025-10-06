package be.ugent.idlab.knows.flink.source.dataio;

import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;

import be.ugent.idlab.knows.amo.operators.source.SourceOperator;

public class FlinkDataIOSplit implements SourceSplit, Serializable{

    private SourceOperator sourceOperator;

    public FlinkDataIOSplit(SourceOperator sourceOperator) {
        this.sourceOperator = sourceOperator;
    }

    @Override
    public String splitId() {
        return this.sourceOperator.getOperatorName();
    }

    public SourceOperator getSourceOperator() {
        return sourceOperator;
    }
}
