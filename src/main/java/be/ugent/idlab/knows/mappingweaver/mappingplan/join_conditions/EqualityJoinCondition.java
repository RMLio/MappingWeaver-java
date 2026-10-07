package be.ugent.idlab.knows.mappingweaver.mappingplan.join_conditions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.JoinCondition;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;

import java.util.Map;

public record EqualityJoinCondition(Map<String, String> attributePairs) implements JoinCondition {

    /**
     * A condition compares the attributes it is given, so it needs at least one pair.
     * <p>
     * Without a pair there is nothing to compare and every record would join every other
     * one, which is a cross join and not what a mapping asking for an equality meant. A
     * plan that cannot express its condition — one joining on a constant, for instance —
     * arrives here with no pairs at all, and is refused rather than silently answered with
     * every combination.
     */
    public EqualityJoinCondition {
        if (attributePairs.isEmpty()) {
            throw new MappingException(
                    "A join on equality has no attributes to compare. The mapping plan holds an "
                            + "InnerJoin whose condition is empty, so the attributes it should join "
                            + "on are missing from the plan.");
        }
    }

    @Override
    public boolean applyCheck(SolutionMapping leftSolMap, SolutionMapping rightSolMap) {
        for (Map.Entry<String, String> entry : this.attributePairs.entrySet()) {
            String left = entry.getKey();
            String right = entry.getValue();

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
