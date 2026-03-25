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

import java.io.Serializable;
import java.util.List;
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

    @Override
    public @Nullable String apply(@Nullable SolutionMapping solutionMapping) {
        Arguments arguments = new Arguments();
        for (int i = 0; i < this.parameters.size(); i++) {
            FnOParameter arg = this.parameters.get(i);
            String predicate = PARAMETER_TRANSLATOR.translate(arg.getIdentifier());
            arguments.add(predicate, arg.getParameter(solutionMapping));
        }

        // TODO: what is the expected behaviour? RMLFNML test cases expect errors to be thrown...
                try {
            // extract the value from the Agent
            Object result = getAgent().execute(this.identifier, arguments);
            if (result == null) {
                return null;
            }
            // Convert result to String - handles both String and other types (Integer, etc.)
            return result.toString();
        } catch (Exception e) { // TODO: replace the generic exception
            throw new RuntimeException(e);
        }
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
