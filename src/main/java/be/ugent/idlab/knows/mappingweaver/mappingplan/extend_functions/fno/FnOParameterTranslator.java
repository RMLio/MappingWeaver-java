package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.vocabulary.RDF;

/**
 * Builds a translation map that converts FnO parameter identifiers into the
 * predicate IRIs expected by the function agent.
 */
final class FnOParameterTranslator {

    private static final String FNO_NAMESPACE = "https://w3id.org/function/ontology#";

    private final Map<String, String> parameterToPredicate;

    FnOParameterTranslator(String[] functionDescriptions) {
        this.parameterToPredicate = loadTranslations(functionDescriptions);
    }

    /**
     * Translate the given parameter identifier into the predicate IRI that should
     * be passed to the agent. If the identifier is unknown, the original identifier
     * (without angle brackets) is returned.
     */
    String translate(String identifier) {
        if (identifier == null) {
            return null;
        }

        String trimmed = identifier.trim();

        String predicate = parameterToPredicate.get(trimmed);
        if (predicate != null) {
            return predicate;
        }

        predicate = parameterToPredicate.get(trimmed);
        if (predicate != null) {
            return predicate;
        }

        return trimmed;
    }

    private static Map<String, String> loadTranslations(String[] functionDescriptions) {
        Map<String, String> map = new HashMap<>();
        for (String description : functionDescriptions) {
            try (InputStream in = openResource(description)) {
                if (in == null) {
                    throw new IllegalStateException("FnO description '" + description + "' not found on the classpath.");
                }
                Model model = ModelFactory.createDefaultModel();
                model.read(in, null, "TURTLE");

                Resource parameterType = model.createResource(FNO_NAMESPACE + "Parameter");
                Property predicateProperty = model.createProperty(FNO_NAMESPACE + "predicate");

                ResIterator parameters = model.listResourcesWithProperty(RDF.type, parameterType);
                while (parameters.hasNext()) {
                    Resource parameter = parameters.next();
                    Statement predicateStmt = parameter.getProperty(predicateProperty);
                    if (predicateStmt == null || !predicateStmt.getObject().isResource()) {
                        continue;
                    }
                    String predicateUri = predicateStmt.getResource().getURI();
                    registerKeys(map, model, parameter, predicateUri);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load FnO description '" + description + "'", e);
            }
        }
        return map;
    }

    private static void registerKeys(Map<String, String> map, Model model, Resource parameter, String predicateUri) {
        String uri = parameter.getURI();
        addIfPresent(map, uri, predicateUri);
        addIfPresent(map, "<" + uri + ">", predicateUri);

        String prefixedName = model.qnameFor(uri);
        if (prefixedName != null) {
            addIfPresent(map, prefixedName, predicateUri);
            int colon = prefixedName.indexOf(':');
            if (colon >= 0 && colon + 1 < prefixedName.length()) {
                addIfPresent(map, prefixedName.substring(colon + 1), predicateUri);
            }
        } else if (parameter.getLocalName() != null) {
            addIfPresent(map, parameter.getLocalName(), predicateUri);
        }
    }

    private static void addIfPresent(Map<String, String> map, String key, String value) {
        if (key == null || key.isEmpty()) {
            return;
        }
        map.putIfAbsent(key, value);
    }

    private static InputStream openResource(String name) {
        ClassLoader loader = FnOParameterTranslator.class.getClassLoader();
        InputStream in = loader.getResourceAsStream(name);
        if (in != null) {
            return in;
        }
        return loader.getResourceAsStream("/" + name);
    }

}

