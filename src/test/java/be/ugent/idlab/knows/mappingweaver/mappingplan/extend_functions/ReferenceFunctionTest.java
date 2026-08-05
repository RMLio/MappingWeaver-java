package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.NullNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A reference to something the record does not have is NULL, not an error.
 * <p>
 * The RML-IO registry requires it of the JSONPath reference formulation: a path referring
 * to a non-existent name or child of the input value yields NULL. The term that would have
 * used the value is then not generated, and the rest of the record is still mapped.
 */
public class ReferenceFunctionTest {

    private static SolutionMapping mappingWith(String key, be.ugent.idlab.knows.amo.blocks.nodes.RDFNode value) {
        SolutionMapping mapping = new SolutionMapping();
        mapping.put(key, value);
        return mapping;
    }

    @Test
    public void anAttributeTheRecordDoesNotHaveIsNull() {
        SolutionMapping mapping = mappingWith("name", new LiteralNode("alice"));

        assertNull(new ReferenceFunction("scope").apply(mapping));
    }

    @Test
    public void anAttributeBoundToNullIsNull() {
        SolutionMapping mapping = mappingWith("scope", new NullNode());

        assertNull(new ReferenceFunction("scope").apply(mapping));
    }

    @Test
    public void anEmptyValueIsAValue() {
        // an empty string is data, not an absent attribute
        SolutionMapping mapping = mappingWith("scope", new LiteralNode(""));

        assertEquals("", new ReferenceFunction("scope").apply(mapping));
    }

    @Test
    public void aBoundAttributeIsRead() {
        SolutionMapping mapping = mappingWith("scope", new LiteralNode("read write"));

        assertEquals("read write", new ReferenceFunction("scope").apply(mapping));
    }

    @Test
    public void thereIsNothingToReadWithoutASolutionMapping() {
        assertNull(new ReferenceFunction("scope").apply(null));
    }

    @Test
    public void applyToNodeIsNullForAnAttributeTheRecordDoesNotHave() {
        SolutionMapping mapping = mappingWith("name", new LiteralNode("alice"));

        assertNull(new ReferenceFunction("scope").applyToNode(mapping));
    }
}
