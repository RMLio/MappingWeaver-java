package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno;

import java.io.Serializable;

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

    public String getIdentifier() {
        return identifier;
    }
}
