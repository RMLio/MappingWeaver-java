package be.ugent.idlab.knows.mappingweaver.mappingplan.join_conditions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EqualityJoinConditionTest {

    private static SolutionMapping mapping(String variable, String value) {
        SolutionMapping solutionMapping = new SolutionMapping();
        solutionMapping.put(variable, new LiteralNode(value));
        return solutionMapping;
    }

    @Test
    public void mappingsWhoseAttributesAreEqualJoin() {
        EqualityJoinCondition condition = new EqualityJoinCondition(Map.of("?sport", "?id"));

        assertTrue(condition.applyCheck(mapping("?sport", "100"), mapping("?id", "100")));
    }

    @Test
    public void mappingsWhoseAttributesDifferDoNotJoin() {
        EqualityJoinCondition condition = new EqualityJoinCondition(Map.of("?sport", "?id"));

        assertFalse(condition.applyCheck(mapping("?sport", "100"), mapping("?id", "200")));
    }

    @Test
    public void aMappingWithoutTheAttributeDoesNotJoin() {
        EqualityJoinCondition condition = new EqualityJoinCondition(Map.of("?sport", "?id"));

        assertFalse(condition.applyCheck(mapping("?other", "100"), mapping("?id", "100")));
    }

    @Test
    public void aConditionWithNothingToCompareIsRefused() {
        // every record would otherwise join every other one: the loop over the attributes
        // finds nothing to reject on and answers true. A plan whose join condition could
        // not be expressed - joining on a constant, for instance - arrives here empty, and
        // a cross join is not what the mapping asked for.
        assertThrows(MappingException.class, () -> new EqualityJoinCondition(Map.of()));
    }
}
