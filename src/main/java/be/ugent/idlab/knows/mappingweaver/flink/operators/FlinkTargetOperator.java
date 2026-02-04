package be.ugent.idlab.knows.mappingweaver.flink.operators;

import java.util.Set;

import org.json.JSONObject;
import org.jspecify.annotations.NonNull;

import be.ugent.idlab.knows.amo.operators.OperatorVisitor;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.WeaverSinkFactory;

public class FlinkTargetOperator extends TargetOperator {
    private JSONObject config;

    public FlinkTargetOperator(String operatorName, Set<String> inputFragments, String targetVariable,
            JSONObject config) {
        super(operatorName, inputFragments, targetVariable, null);
        this.config = config;
    }

    @Override
    public <T> T accept(@NonNull OperatorVisitor<@NonNull T> visitor) {
        return visitor.visitTarget(this);
    }

    public WeaverSinkFactory getSinkFactory() {
        return new WeaverSinkFactory(this.config, this.getOperatorName(), this.getTargetVariable());

    }

    public JSONObject getConfig() {
        return config;
    }

}
