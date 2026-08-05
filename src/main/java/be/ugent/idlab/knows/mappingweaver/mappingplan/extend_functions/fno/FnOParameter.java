package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;


public class FnOParameter implements Serializable {

    private final String identifier;
    private final ExtendFunction innerFunction;
    public FnOParameter(String identifier, ExtendFunction innerFunction) {
        this.identifier = identifier;
        this.innerFunction = innerFunction;
    }

    public String getParameter(SolutionMapping solutionMapping) {
        return innerFunction.apply(solutionMapping);
    }

    /**
     * Every value this parameter stands for. A parameter is usually a single value, but
     * the function filling it in may produce several (a split, for instance), and then the
     * function this parameter belongs to is run once for each of them.
     *
     * @param solutionMapping the solution mapping to evaluate against
     * @return the parameter's values; a single {@code null} when it has no value, so that
     *         the function is still run with the parameter unset, as it was before
     */
    public List<String> getParameters(SolutionMapping solutionMapping) {
        List<String> values = innerFunction.applyMulti(solutionMapping);

        return values.isEmpty() ? Collections.singletonList(null) : values;
    }

    public String getIdentifier() {
        return identifier;
    }
}
