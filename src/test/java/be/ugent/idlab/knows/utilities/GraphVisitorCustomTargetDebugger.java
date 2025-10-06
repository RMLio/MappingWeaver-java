package be.ugent.idlab.knows.utilities;

import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.values.MapTupValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.jspecify.annotations.NonNull;

public class GraphVisitorCustomTargetDebugger extends GraphVisitorCustomTarget{
    public GraphVisitorCustomTargetDebugger(StreamExecutionEnvironment env, OperatorGraph graph, String targetVariable) {
        super(env, graph, targetVariable);
    }

    // Change all streams so they print the input output
    public void activateDebugging(){
        this.streamCache.replaceAll((k, v) -> {
            String operatorName = k.getOperatorName();
            DataStream<MapTupValue> printingStream = v.map(value -> {
                System.out.println("Operator: " + operatorName + " Output: " + value.getValue());
                return value;
            });
            return printingStream;
        });
    }

    @Override
    public Void visitTarget(@NonNull TargetOperator targetOperator) {
        activateDebugging();
        return super.visitTarget(targetOperator);
    }
}
