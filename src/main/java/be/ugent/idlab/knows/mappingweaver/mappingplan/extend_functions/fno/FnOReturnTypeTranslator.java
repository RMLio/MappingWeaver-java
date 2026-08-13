package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Retrieves the return datatype for FnO functions from the function descriptions.
 */
final class FnOReturnTypeTranslator {

    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final Logger LOG = LoggerFactory.getLogger(FnOReturnTypeTranslator.class);
    private final Map<String, String> outputToDatatype;
    private final Map<String, List<String>> functionToOutputs;

    FnOReturnTypeTranslator(Model model) {
        this.functionToOutputs = new HashMap<>();
        loadOutputs(model, functionToOutputs);
        this.outputToDatatype = loadOutputDatatypes(model, functionToOutputs.values());
    }

    String resolveOutputDatatype(String functionIdentifier, String requestedOutput) {
        String function = normalize(functionIdentifier);
        String requested = normalize(requestedOutput);
        List<String> outputs = functionToOutputs.get(function);
        if (outputs == null) {
            LOG.warn("FnO function '{}' has no declared fno:returns list; cannot validate rml:return '{}'.",
                    functionIdentifier, requestedOutput);
            return XSD_STRING;
        }
        if (requested == null) {
            LOG.debug("FnO function '{}' has no rml:return; using its first declared return resource.",
                    functionIdentifier);
        }
        boolean validRequestedOutput = requested != null && outputs.contains(requested);
        if (requested != null && !validRequestedOutput) {
            LOG.warn("Return resource '{}' is not declared by FnO function '{}'; using its first declared return resource.",
                    requestedOutput, functionIdentifier);
        }
        String resolvedOutput = validRequestedOutput
                ? requested
                : outputs.isEmpty() ? null : outputs.get(0);
        return outputToDatatype.getOrDefault(resolvedOutput, XSD_STRING);
    }

    private static String normalize(String identifier) {
        if (identifier == null) {
            return null;
        }
        // FnO identifiers can arrive as bare IRIs or RDF/SPARQL-style <IRI> terms.
        // Treat those equivalent lexical forms as the same resource key.
        String trimmed = identifier.trim();
        if (trimmed.startsWith("<") && trimmed.endsWith(">")) {
            return trimmed.substring(1, trimmed.length() - 1);
        }
        return trimmed;
    }

    private Map<String, String> loadOutputDatatypes(Model model, Collection<List<String>> functionOutputs) {
        Map<String, String> map = new HashMap<>();
        Set<String> outputIdentifiers = new HashSet<>();
        for (List<String> outputs : functionOutputs) {
            outputIdentifiers.addAll(outputs);
        }

        String query = "PREFIX fno: <https://w3id.org/function/ontology#> " +
                      "SELECT ?out ?type WHERE { " +
                      "  ?out fno:type ?type " +
                      "}";
        try {
            org.apache.jena.query.Query q = org.apache.jena.query.QueryFactory.create(query);
            try (org.apache.jena.query.QueryExecution qexec = org.apache.jena.query.QueryExecutionFactory.create(q, model)) {
                org.apache.jena.query.ResultSet results = qexec.execSelect();
                while (results.hasNext()) {
                    org.apache.jena.query.QuerySolution solution = results.nextSolution();
                    Resource output = solution.getResource("out");
                    RDFNode type = solution.get("type");
                    if (output != null && outputIdentifiers.contains(output.getURI())
                            && type != null && type.isResource()) {
                        map.put(output.getURI(), type.asResource().getURI());
                    }
                }
            }
        } catch (Exception e) {
            // If SPARQL query fails, fall back to default
        }
        return map;
    }

    private void loadOutputs(Model model, Map<String, List<String>> outputsByFunction) {
        org.apache.jena.rdf.model.Property returns = model.createProperty(
                "https://w3id.org/function/ontology#returns");
        StmtIterator statements = model.listStatements(null, returns, (RDFNode) null);
        try {
            while (statements.hasNext()) {
                var statement = statements.nextStatement();
                if (!statement.getSubject().isURIResource() || !statement.getObject().isResource()) {
                    continue;
                }

                // SPARQL property paths find every list member but do not guarantee RDF-list
                // order. Traverse this list directly because the first member is semantic.
                Resource list = statement.getResource();
                List<String> outputs = new ArrayList<>();
                while (!RDF.nil.equals(list)) {
                    Resource output = list.getPropertyResourceValue(RDF.first);
                    if (output == null) {
                        break;
                    }
                    outputs.add(output.getURI());
                    list = list.getPropertyResourceValue(RDF.rest);
                    if (list == null) {
                        break;
                    }
                }
                outputsByFunction.put(statement.getSubject().getURI(), outputs);
            }
        } finally {
            statements.close();
        }
    }
}
