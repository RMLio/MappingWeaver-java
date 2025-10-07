package be.ugent.idlab.knows.mappingweaver.mappingplan.join_conditions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.JoinCondition;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public record EqualityJoinCondition(Map<String, String> attributePairs) implements JoinCondition {

    private static final Logger LOG = LoggerFactory.getLogger(EqualityJoinCondition.class);

    @Override
    public boolean applyCheck(SolutionMapping leftSolMap, SolutionMapping rightSolMap) {
        LOG.warn("Left solution mapping {}", leftSolMap.toString());
        LOG.warn("Right solution mapping {}", rightSolMap.toString());

        for (Map.Entry<String, String> entry : this.attributePairs.entrySet()) {
            String left = entry.getKey();
            String right = entry.getValue();
            LOG.warn("Left attribute: {}", left);
            LOG.warn("Right attribute: {}", right);

            RDFNode leftValue = leftSolMap.get(left);
            RDFNode rightValue = rightSolMap.get(right);

            if (leftValue == null) {
                return false;
            }

            if (!leftValue.equals(rightValue)) {
                return false;
            }

        }
        return true;
    }
}
