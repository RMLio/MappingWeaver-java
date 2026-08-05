package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;
import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Function that applies the specified FnO function to the input
 */
public class FnOFunction implements ExtendFunction, Serializable {

    private static final String[] FUNCTION_DESCRIPTIONS = new String[]{
            "functions_grel.ttl",
            "grel_java_mapping.ttl",
            "functions_idlab.ttl",
            "functions_idlab_classes_java_mapping.ttl",
    };

    private static final FnOParameterTranslator PARAMETER_TRANSLATOR =
            new FnOParameterTranslator(FUNCTION_DESCRIPTIONS);
    
    private static final FnOReturnTypeTranslator RETURN_TYPE_TRANSLATOR =
            new FnOReturnTypeTranslator(FUNCTION_DESCRIPTIONS);

    private static final Logger LOG = LoggerFactory.getLogger(FnOFunction.class);

    private static final String XSD_STRING = "http://www.w3.org/2001/XMLSchema#string";

    // Lazy initialization of the Agent (not serialized, created per JVM instance)
    private static volatile Agent cachedAgent = null;
    private static final Object agentLock = new Object();

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
        this.datatypeIRI = RETURN_TYPE_TRANSLATOR.getDatatype(identifier);
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
                        cachedAgent = AgentFactory.createFromFnO(FUNCTION_DESCRIPTIONS);
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
            predicates.add(PARAMETER_TRANSLATOR.translate(parameter.getIdentifier()));
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
        if (values.size() == 1) {
            return List.of(new LiteralNode(values.get(0), datatypeIRI, ""));
        }

        // The declared datatype describes what the function returns as a whole, which for
        // a function producing several values is the collection (rdf:List) and not the
        // values in it. Each of them is a string.
        return values.stream()
                .map(value -> (RDFNode) new LiteralNode(value, XSD_STRING, ""))
                .toList();
    }
    
    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Literal);
    }
}
