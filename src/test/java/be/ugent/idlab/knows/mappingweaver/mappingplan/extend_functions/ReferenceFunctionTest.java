package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Reading an attribute of the record.
 * <p>
 * {@code applyToNode} gives the node the record holds and is what the operators generating
 * terms call; {@code apply} is that node as a string. A reference to something the record
 * does not have is NULL rather than an error: the RML-IO registry requires it of the
 * JSONPath reference formulation, so the term that would have used the value is not
 * generated and the rest of the record is still mapped.
 */
public class ReferenceFunctionTest {

    private static SolutionMapping mappingWith(String key, RDFNode value) {
        SolutionMapping mapping = new SolutionMapping();
        mapping.put(key, value);
        return mapping;
    }

    @Test
    public void theNodeTheRecordHoldsIsGivenAsItIs() {
        // this is why applyToNode is overridden: the inherited default runs the value
        // through apply() and rebuilds it as a Literal, which would turn a reference to an
        // IRI into a string
        SolutionMapping mapping = mappingWith("scope", new IRINode("http://example.com/read"));

        RDFNode node = new ReferenceFunction("scope", false).applyToNode(mapping);

        assertInstanceOf(IRINode.class, node);
        assertEquals("http://example.com/read", node.getValue().toString());
    }

    @Test
    public void aBlankNodeStaysABlankNode() {
        SolutionMapping mapping = mappingWith("scope", new BlankNode("b1"));

        assertInstanceOf(BlankNode.class, new ReferenceFunction("scope", false).applyToNode(mapping));
    }

    @Test
    public void anAttributeTheRecordDoesNotHaveIsNull() {
        SolutionMapping mapping = mappingWith("name", new LiteralNode("alice"));

        assertNull(new ReferenceFunction("scope", true).applyToNode(mapping));
        assertNull(new ReferenceFunction("scope", true).apply(mapping));
    }

    @Test
    public void anAttributeBoundToNullIsNull() {
        SolutionMapping mapping = mappingWith("scope", new NullNode());

        assertNull(new ReferenceFunction("scope", false).applyToNode(mapping));
        assertNull(new ReferenceFunction("scope", false).apply(mapping));
    }

    @Test
    public void thereIsNothingToReadWithoutASolutionMapping() {
        assertNull(new ReferenceFunction("scope", false).applyToNode(null));
        assertNull(new ReferenceFunction("scope", false).apply(null));
    }

    @Test
    public void anEmptyValueIsAValue() {
        // an empty string is data, not an absent attribute
        SolutionMapping mapping = mappingWith("scope", new LiteralNode(""));

        assertEquals("", new ReferenceFunction("scope", false).apply(mapping));
    }

    @Test
    public void aBoundAttributeIsReadAsAString() {
        SolutionMapping mapping = mappingWith("scope", new LiteralNode("read write"));

        assertEquals("read write", new ReferenceFunction("scope", false).apply(mapping));
    }
}
