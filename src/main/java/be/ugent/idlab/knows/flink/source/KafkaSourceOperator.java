package be.ugent.idlab.knows.flink.source;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.operators.OperatorVisitor;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.DataIOSourceOperator;
import org.apache.commons.lang3.NotImplementedException;
import org.jspecify.annotations.NonNull;

import java.util.Set;

public class KafkaSourceOperator extends SourceOperator {

    private final String groupId;
    private final String topic;
    private final String brokers;
    private final DataIOSourceOperator underlyingOperator;

    public KafkaSourceOperator(String operatorName, String groupId, String topic, String brokers, DataIOSourceOperator underlyingOperator, Set<String> outputFragments) {
        super(operatorName, outputFragments);
        this.groupId = groupId;
        this.topic = topic;
        this.brokers = brokers;
        this.underlyingOperator = underlyingOperator;
    }

    @Override
    protected MappingTuple nextEffective() {
        throw new NotImplementedException("This operator does not support nextEffective()");
    }

    @Override
    public boolean hasNext() {
        return underlyingOperator.hasNext();
    }

    @Override
    public MappingTuple consumeSource() {
        return underlyingOperator.consumeSource();
    }


    @Override
    public void init() throws Exception {
        underlyingOperator.init();
    }

    @Override
    public <T> @NonNull T accept(@NonNull OperatorVisitor<T> operatorVisitor) {
        return operatorVisitor.visitSource(this);
    }

    public String getBrokers() {
        return brokers;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getTopic() {
        return topic;
    }

    public DataIOSourceOperator getUnderlyingOperator() {
        return underlyingOperator;
    }
}
