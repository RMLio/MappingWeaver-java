package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.Resource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

class FnOReturnTypeTranslatorTest {

    private static final String FNO_RETURNS = "https://w3id.org/function/ontology#returns";
    private static final String FNO_TYPE = "https://w3id.org/function/ontology#type";
    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final String XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer";

    @Test
    void resolvesANonFirstDeclaredOutputAndFallsBackToTheFirst() {
        Model model = ModelFactory.createDefaultModel();
        Resource function = model.createResource("https://example.org/function");
        Resource firstOutput = model.createResource("https://example.org/firstOutput");
        Resource secondOutput = model.createResource("https://example.org/secondOutput");
        firstOutput.addProperty(model.createProperty(FNO_TYPE), model.createResource(XSD_STRING));
        secondOutput.addProperty(model.createProperty(FNO_TYPE), model.createResource(XSD_INTEGER));
        RDFList returns = model.createList(new Resource[]{firstOutput, secondOutput});
        function.addProperty(model.createProperty(FNO_RETURNS), returns);

        FnOReturnTypeTranslator translator = new FnOReturnTypeTranslator(model);

        assertEquals(XSD_INTEGER, translator.resolveOutputDatatype(function.getURI(), secondOutput.getURI()));
        assertEquals(XSD_STRING, translator.resolveOutputDatatype(function.getURI(), "https://example.org/invalidOutput"));
    }
}
