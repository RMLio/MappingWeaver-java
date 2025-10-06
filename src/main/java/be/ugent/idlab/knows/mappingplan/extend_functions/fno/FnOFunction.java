package be.ugent.idlab.knows.mappingplan.extend_functions.fno;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.functions.agent.Agent;
import be.ugent.idlab.knows.functions.agent.AgentFactory;
import be.ugent.idlab.knows.functions.agent.Arguments;
import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;
import org.jspecify.annotations.Nullable;

import java.io.Serializable;
import java.util.List;

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

    private final String identifier;
    private final List<FnOParameter> parameters;
//    private final Agent agent;

    public FnOFunction(String identifier, List<FnOParameter> parameters) throws FnOException {
        this.identifier = identifier;
        this.parameters = parameters;
//        this.agent = AgentFactory.createFromFnO(FUNCTION_DESCRIPTIONS);
    }


    @Override
    public @Nullable String apply(@Nullable SolutionMapping solutionMapping) {
        Arguments arguments = new Arguments();
        this.parameters.forEach(arg -> arguments.add(arg.getIdentifier(), arg.getParameter(solutionMapping)));

        try (Agent agent = AgentFactory.createFromFnO(FUNCTION_DESCRIPTIONS)) {
            // extract the value from
            return (String) agent.execute(this.identifier, arguments);
        } catch (Exception e) { // TODO: replace the generic exception
            throw new RuntimeException(e);
        }
    }
}
