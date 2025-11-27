package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class FnOParameterTranslatorTest {


    @Test
    void translatesKnownParameterToPredicate() {

    String valueParam =
            "http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParam";

    String valuePredicate =
            "http://users.ugent.be/~bjdmeest/function/grel.ttl#valueParameter";

        FnOParameterTranslator translator = new FnOParameterTranslator(new String[]{"functions_grel.ttl"});

        assertEquals(valuePredicate, translator.translate(valueParam));
    }

    @Test
    void returnsNormalizedIdentifierWhenUnknown() {
        FnOParameterTranslator translator = new FnOParameterTranslator(new String[]{"functions_grel.ttl"});
        String unknown = "http://example.com/unknown";
        assertEquals(unknown, translator.translate(unknown));
    }
}

