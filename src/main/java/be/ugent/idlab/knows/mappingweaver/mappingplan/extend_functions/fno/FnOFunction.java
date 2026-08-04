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
    private @Nullable Object execute(@Nullable SolutionMapping solutionMapping) {
        Arguments arguments = new Arguments();
        for (int i = 0; i < this.parameters.size(); i++) {
            FnOParameter arg = this.parameters.get(i);
            String predicate = PARAMETER_TRANSLATOR.translate(arg.getIdentifier());
            arguments.add(predicate, arg.getParameter(solutionMapping));
        }

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
        List<String> values = valuesOf(execute(solutionMapping));
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
        return valuesOf(execute(solutionMapping));
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
        // If return_type is unknownOut or doesn't match expected, return null (no output)
        if (returnType != null && returnType.contains("unknownOut")) {
            return null;
        }
        
        String value = apply(solutionMapping);
        if (value == null) {
            return null;
        }
        // Return a LiteralNode with the appropriate datatype
        return new LiteralNode(value, datatypeIRI, "");
    }
    
    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Literal);
    }
}
