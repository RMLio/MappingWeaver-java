package be.ugent.idlab.knows.mappingplan;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.values.MapTupValue;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * A wrapper around the TargetOperator implementing SinkFunction.
 * Will be placed at the end of the stream to consume all incoming values into the sink specified by the operator.
 */
public class TargetSinkFunction implements SinkFunction<MapTupValue> {

    private final TargetOperator operator;

    public TargetSinkFunction(TargetOperator operator) {
        this.operator = operator;
    }

    @Override
    public void invoke(MapTupValue value, Context context) {
        if (value != null && value.getValue() != null) {
            this.operator.apply(value.getValue());
        }

    }
}
