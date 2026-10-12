package be.ugent.idlab.knows.mappingweaver.components.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.IriTypeFunction;
import be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.ReferenceFunction;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Generating an IRI term from a value.
 * <p>
 * RML-Core: a value that is an absolute IRI is the IRI; any other value is prepended with the
 * base IRI, and if that gives no valid IRI, no term is generated. An absolute IRI is one with a
 * scheme (any scheme) and optionally a fragment, as in RDF; percent-encoded characters are
 * allowed anywhere in the path.
 */
public class IriTypeFunctionTest {

    private static final String BASE = "http://example.com/";

    private static List<String> iris(String value) {
        SolutionMapping mapping = new SolutionMapping();
        mapping.put("name", new LiteralNode(value));
        List<RDFNode> nodes = new IriTypeFunction(BASE, new ReferenceFunction("name", false)).apply(mapping);
        assertNotNull(nodes);
        return nodes.stream().map(node -> node.getValue().toString()).toList();
    }

    @Test
    public void anAbsoluteIriOfAnySchemeIsUsedAsItIs() {
        assertEquals(List.of("urn:isbn:9780262510875"), iris("urn:isbn:9780262510875"));
        assertEquals(List.of("mailto:alice@example.com"), iris("mailto:alice@example.com"));
        assertEquals(List.of("http://example.com/company/Alice"), iris("http://example.com/company/Alice"));
        assertEquals(List.of("http://example.com/vocab#Alice"), iris("http://example.com/vocab#Alice"));
    }

    @Test
    public void aRelativeValueIsPrependedWithTheBaseIri() {
        assertEquals(List.of("http://example.com/Bob"), iris("Bob"));
    }

    @Test
    public void percentEncodedSlashesInThePathAreKept() {
        // RMLTC0020a: a template value that was an IRI itself, percent-encoded
        assertEquals(List.of("http://example.com/http%3A%2F%2Fexample.com%2Fcompany%2FAlice"),
                iris("http%3A%2F%2Fexample.com%2Fcompany%2FAlice"));
    }

    @Test
    public void aValueThatGivesNoValidIriGeneratesNoTerm() {
        assertEquals(List.of(), iris("Emily Smith"));
    }
}
