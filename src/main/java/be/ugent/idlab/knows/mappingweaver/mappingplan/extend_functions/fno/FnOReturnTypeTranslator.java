package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

/**
 * Retrieves the return datatype for FnO functions from the function descriptions.
 */
final class FnOReturnTypeTranslator {

    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private final Map<String, String> functionToDatatype;

    FnOReturnTypeTranslator(Model model) {
        this.functionToDatatype = loadDatatypes(model);
    }

    String getDatatype(String functionIdentifier) {
        if (functionIdentifier == null) {
            return XSD_STRING;
        }
        String trimmed = functionIdentifier.trim();
        if (trimmed.startsWith("<") && trimmed.endsWith(">")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        return functionToDatatype.getOrDefault(trimmed, XSD_STRING);
    }

    private Map<String, String> loadDatatypes(Model model) {
        Map<String, String> map = new HashMap<>();

        // Query: SELECT ?func ?type WHERE { ?func fno:returns ?ret . ?ret rdf:first ?out . ?out fno:type ?type }
        String query = "PREFIX fno: <https://w3id.org/function/ontology#> " +
                      "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> " +
                      "SELECT ?func ?type WHERE { " +
                      "  ?func fno:returns ?ret . " +
                      "  ?ret rdf:first ?out . " +
                      "  ?out fno:type ?type " +
                      "}";

        try {
            org.apache.jena.query.Query q = org.apache.jena.query.QueryFactory.create(query);
            try (org.apache.jena.query.QueryExecution qexec = org.apache.jena.query.QueryExecutionFactory.create(q, model)) {
                org.apache.jena.query.ResultSet results = qexec.execSelect();
                while (results.hasNext()) {
                    org.apache.jena.query.QuerySolution soln = results.nextSolution();
                    Resource func = soln.getResource("func");
                    RDFNode type = soln.get("type");
                    if (func != null && type != null && type.isResource()) {
                        map.put(func.getURI(), type.asResource().getURI());
                    }
                }
            }
        } catch (Exception e) {
            // If SPARQL query fails, fall back to default
        }

        return map;
    }
}
