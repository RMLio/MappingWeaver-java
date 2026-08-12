package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class FnOParameterTranslatorTest {

    private static Model grelModel;

    @BeforeAll
    static void loadModel() {
        grelModel = ModelFactory.createDefaultModel();
        try (var in = FnOParameterTranslatorTest.class.getClassLoader().getResourceAsStream("functions_grel.ttl")) {
            grelModel.read(in, null, "TURTLE");
        } catch (Exception e) {
            throw new RuntimeException("Could not load functions_grel.ttl from classpath", e);
        }
    }

    @Test
    void translatesKnownParameterToPredicate() {
        String valueParam = "http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam";
        String valuePredicate = "http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter";

        FnOParameterTranslator translator = new FnOParameterTranslator(grelModel);

        assertEquals(valuePredicate, translator.translate(valueParam));
    }

    @Test
    void returnsNormalizedIdentifierWhenUnknown() {
        FnOParameterTranslator translator = new FnOParameterTranslator(grelModel);
        String unknown = "http://example.com/unknown";
        assertEquals(unknown, translator.translate(unknown));
    }
}

