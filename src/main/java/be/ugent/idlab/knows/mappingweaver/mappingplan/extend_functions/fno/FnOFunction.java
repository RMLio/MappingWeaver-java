package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.io.InputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.ResIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.riot.RiotException;
import org.apache.jena.vocabulary.RDF;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;
import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;

/**
 * Function that applies the specified FnO function to the input
 */
public class FnOFunction implements ExtendFunction, Serializable {

    private static final class OpenedResource {
        private final InputStream inputStream;
        private final String source;

        private OpenedResource(InputStream inputStream, String source) {
            this.inputStream = inputStream;
            this.source = source;
        }
    }

    private static final String CLASSPATH_PREFIX = "classpath://";

    private static final String[] DEFAULT_FUNCTION_DESCRIPTIONS = new String[]{
            "classpath://functions_grel.ttl",
            "classpath://grel_java_mapping.ttl",
            "classpath://functions_idlab.ttl",
            "classpath://functions_idlab_classes_java_mapping.ttl",
    };

    // mutable only via configure(); volatile for safe cross-thread reads
    private static volatile String[] effectiveDescriptions = DEFAULT_FUNCTION_DESCRIPTIONS;
    private static volatile FnOParameterTranslator parameterTranslator = null;
    private static volatile FnOReturnTypeTranslator returnTypeTranslator = null;

    private static final String FNO_FUNCTION_TYPE = "https://w3id.org/function/ontology#Function";

    private static final Logger LOG = LoggerFactory.getLogger(FnOFunction.class);

    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";
    private static final String RDF_LIST = "http://www.w3.org/1999/02/22-rdf-syntax-ns#List";

    // not serialized; recreated per JVM instance
    private static volatile Agent cachedAgent = null;
    private static volatile Model effectiveModel = null;
    private static final Object agentLock = new Object();

    /**
     * Configures the set of FnO function descriptions used by all FnOFunction instances.
     * Must be called before the mapping plan is parsed.
     *
     * @param customDescriptions  additional description paths (file path or URL)
     * @param customFunctionsOnly when true, only the provided descriptions are used;
     *                            when false (default), they are appended to the built-ins
     */
    public static void configure(List<String> customDescriptions, boolean customFunctionsOnly) {
        synchronized (agentLock) {
            String[] custom = customDescriptions.toArray(new String[0]);
            java.util.Set<String> customSet = new java.util.HashSet<>(customDescriptions);
            effectiveDescriptions = customFunctionsOnly
                    ? custom
                    : Stream.concat(
                        Arrays.stream(DEFAULT_FUNCTION_DESCRIPTIONS).filter(d -> {
                            String stripped = d.startsWith(CLASSPATH_PREFIX) ? d.substring(CLASSPATH_PREFIX.length()) : d;
                            return !customSet.contains(stripped) && !customSet.contains(d);
                        }),
                        Arrays.stream(custom))
                            .toArray(String[]::new);
            parameterTranslator = null;
            returnTypeTranslator = null;
            cachedAgent = null;
            effectiveModel = null;
        }
    }

    private static Model getEffectiveModel() {
        if (effectiveModel == null) {
            synchronized (agentLock) {
                if (effectiveModel == null) {
                    effectiveModel = buildModel(effectiveDescriptions);
                }
            }
        }
        return effectiveModel;
    }

    private static Model buildModel(String[] descriptions) {
        Model model = ModelFactory.createDefaultModel();
        for (String description : descriptions) {
            OpenedResource opened = openResource(description);
            LOG.debug("Parsing FnO description '{}'", opened != null ? opened.source : abbreviateDescription(description));
            if (opened == null) {
                throw new IllegalStateException("FnO description '" + description + "' not found on the classpath or filesystem.");
            }
            try (InputStream in = opened.inputStream) {
                try {
                    model.read(in, null, "TURTLE");
                } catch (RiotException e) {
                    throw new IllegalStateException("Failed to parse FnO description '" + description + "': " + messageOf(e));
                }
            } catch (IllegalStateException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load FnO description '" + description + "': " + messageOf(e), e);
            }
        }
        return model;
    }

    private static String abbreviateDescription(String description) {
        return description.length() > 80
                ? description.substring(0, 40) + '…' + description.substring(description.length() - 35)
                : description;
    }

    private static String messageOf(Throwable t) {
        String message = t.getMessage();
        return (message == null || message.isBlank()) ? t.toString() : message;
    }

    private static FnOParameterTranslator getParameterTranslator() {
        if (parameterTranslator == null) {
            synchronized (agentLock) {
                if (parameterTranslator == null) {
                    parameterTranslator = new FnOParameterTranslator(getEffectiveModel());
                }
            }
        }
        return parameterTranslator;
    }

    private static FnOReturnTypeTranslator getReturnTypeTranslator() {
        if (returnTypeTranslator == null) {
            synchronized (agentLock) {
                if (returnTypeTranslator == null) {
                    returnTypeTranslator = new FnOReturnTypeTranslator(getEffectiveModel());
                }
            }
        }
        return returnTypeTranslator;
    }

    private final String identifier;
    private final List<FnOParameter> parameters;
    private final String datatypeIRI;
    private final String returnType;
    
    public FnOFunction(String identifier, List<FnOParameter> parameters) throws FnOException {
        this(identifier, parameters, null);
    }
    
    public FnOFunction(String identifier, List<FnOParameter> parameters, String returnType) throws FnOException {
        this.identifier = identifier;
        this.parameters = parameters;
        this.returnType = returnType;
        FnOReturnTypeTranslator translator = getReturnTypeTranslator();
        this.datatypeIRI = translator.resolveOutputDatatype(identifier, returnType);
    }

    /**
     * Gets or creates the cached Agent instance.
     * This is thread-safe and efficient - the Agent is created once and reused.
     */
    private static Agent getAgent() {
        if (cachedAgent == null) {
            synchronized (agentLock) {
                if (cachedAgent == null) {
                    try {
                        // serialize the already-parsed model once rather than re-reading individual files
                        java.io.StringWriter sw = new java.io.StringWriter();
                        getEffectiveModel().write(sw, "TURTLE");
                        cachedAgent = AgentFactory.createFromFnO(sw.toString());
                        if (LOG.isDebugEnabled()) {
                            List<String> discoveredFunctions = discoverFunctions(getEffectiveModel());
                            LOG.debug("Discovered/loaded {} FnO functions: {}", discoveredFunctions.size(), discoveredFunctions);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to create FnO Agent", e);
                    }
                }
            }
        }
        return cachedAgent;
    }

    /**
     * Runs the function through the FnO Agent and returns its raw result, which may be a
     * single value or a collection of them.
     *
     * @param solutionMapping the solution mapping to read the arguments from
     * @return the Agent's result, or null if the function produced no value
     */
    /**
     * Runs the function once for every combination of its parameters' values and returns
     * everything the runs produced.
     * <p>
     * A parameter is usually a single value, but the function filling it in may produce
     * several: splitting a value and passing the result on, for instance. Each of them
     * stands for a record of its own, so the function is applied to every one rather than
     * to the first, and parameters that produce several values multiply out.
     *
     * @param solutionMapping the solution mapping to read the arguments from
     * @return the values produced, empty if the function produced none
     */
    private List<String> allValues(@Nullable SolutionMapping solutionMapping) {
        List<String> predicates = new ArrayList<>(this.parameters.size());
        List<List<String>> valuesPerParameter = new ArrayList<>(this.parameters.size());

        for (FnOParameter parameter : this.parameters) {
            predicates.add(getParameterTranslator().translate(parameter.getIdentifier()));
            valuesPerParameter.add(parameter.getParameters(solutionMapping));
        }

        List<String> produced = new ArrayList<>();
        // one run per combination: an Arguments is built from scratch each time, as it
        // collects the values added under a name rather than replacing them
        int[] chosen = new int[predicates.size()];
        while (true) {
            Arguments arguments = new Arguments();
            for (int i = 0; i < predicates.size(); i++) {
                arguments.add(predicates.get(i), valuesPerParameter.get(i).get(chosen[i]));
            }

            produced.addAll(valuesOf(execute(arguments)));

            int parameter = chosen.length - 1;
            while (parameter >= 0 && ++chosen[parameter] >= valuesPerParameter.get(parameter).size()) {
                chosen[parameter] = 0;
                parameter--;
            }
            if (parameter < 0) {
                return produced;
            }
        }
    }

    private @Nullable Object execute(Arguments arguments) {
        // TODO: what is the expected behaviour? RMLFNML test cases expect errors to be thrown...
        try {
            // extract the value from the Agent
            return getAgent().execute(this.identifier, arguments);
        } catch (FnOException e) {
            // Function could not be resolved (e.g. function not found): a real mapping error.
            throw new RuntimeException(e);
        } catch (Exception e) {
            // The function executed but could not produce a value (e.g. substring index out
            // of range). This is a data error: no triple is generated for this value, but the
            // mapping continues for the rest of the data. The RML spec says to report data
            // errors as early as possible, so log it at ERROR level.
            LOG.error("Function '{}' failed while producing a value; no triple is generated for this record.", this.identifier, e);
            return null;
        }
    }

    @Override
    public @Nullable String apply(@Nullable SolutionMapping solutionMapping) {
        List<String> values = allValues(solutionMapping);
        if (values.isEmpty()) {
            return null;
        }

        // Only one value fits where a single value is expected. A function producing
        // several of them belongs in a place that can carry them all, which is what
        // applyMulti() is for; taking the first is the best that can be done here.
        return values.get(0);
    }

    @Override
    public List<String> applyMulti(@Nullable SolutionMapping solutionMapping) {
        return allValues(solutionMapping);
    }

    /**
     * The values a function's result stands for: a function may produce several (a split,
     * for instance), and it may hand them over as a collection or as an array. GREL's
     * functions return arrays, so unwrapping only collections left the array itself as the
     * value, which stringified to something like {@code [Ljava.lang.String;@1b6d3586}.
     *
     * @param result what the function returned, {@code null} if it produced nothing
     * @return the values it produced, empty if it produced none
     */
    private static List<String> valuesOf(@Nullable Object result) {
        if (result == null) {
            return List.of();
        }

        if (result instanceof Collection<?> values) {
            return values.stream()
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .toList();
        }

        if (result.getClass().isArray()) {
            int length = Array.getLength(result);
            List<String> values = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                Object value = Array.get(result, i);
                if (value != null) {
                    values.add(value.toString());
                }
            }
            return values;
        }

        return List.of(result.toString());
    }

    private static List<String> discoverFunctions(Model model) {
        Set<String> discovered = new TreeSet<>();
        Resource functionType = model.createResource(FNO_FUNCTION_TYPE);
        ResIterator functions = model.listResourcesWithProperty(RDF.type, functionType);
        while (functions.hasNext()) {
            Resource function = functions.next();
            if (function.isURIResource()) {
                discovered.add(function.getURI());
            }
        }
        return List.copyOf(discovered);
    }

    private static @Nullable OpenedResource openResource(String name) {
        if (name.startsWith(CLASSPATH_PREFIX)) {
            String resource = name.substring(CLASSPATH_PREFIX.length());
            return openClasspathResource(resource);
        }
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(name);
            if (java.nio.file.Files.exists(path)) {
                java.nio.file.Path absolutePath = path.toAbsolutePath().normalize();
                return new OpenedResource(java.nio.file.Files.newInputStream(absolutePath), absolutePath.toString());
            }
        } catch (Exception ignored) {}
        return openClasspathResource(name);
    }

    private static @Nullable OpenedResource openClasspathResource(String resourceName) {
        ClassLoader loader = FnOFunction.class.getClassLoader();
        java.net.URL resourceUrl = loader.getResource(resourceName);
        if (resourceUrl == null) {
            return null;
        }
        InputStream in = loader.getResourceAsStream(resourceName);
        if (in == null) {
            return null;
        }
        return new OpenedResource(in, resourceUrl.toString());
    }

    @Override
    public @Nullable RDFNode applyToNode(@Nullable SolutionMapping solutionMapping) {
        List<RDFNode> nodes = applyMultiToNode(solutionMapping);

        return nodes.isEmpty() ? null : nodes.get(0);
    }

    @Override
    public List<RDFNode> applyMultiToNode(@Nullable SolutionMapping solutionMapping) {
        // If return_type is unknownOut or doesn't match expected, return null (no output)
        if (returnType != null && returnType.contains("unknownOut")) {
            return List.of();
        }

        List<String> values = allValues(solutionMapping);
        String valueDatatype = RDF_LIST.equals(datatypeIRI) ? XSD_STRING : datatypeIRI;
        return values.stream()
            .map(value -> (RDFNode) new LiteralNode(value, valueDatatype, ""))
                .toList();
    }
    
    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Literal);
    }
}
