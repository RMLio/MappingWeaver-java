package be.ugent.idlab.knows.mappingplan.extend_functions.fno;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;

import java.io.Serializable;


public class FnOParameter implements Serializable {

    private String identifier;
    private String type;
    private ExtendFunction innerFunction;
    public FnOParameter(String identifier, String type, ExtendFunction innerFunction) {
        this.identifier = identifier;
        this.type = type;
        this.innerFunction = innerFunction;
    }

    public String getParameter(SolutionMapping solutionMapping) {
        return innerFunction.apply(solutionMapping);
    }

    public String getIdentifier() {
        return identifier;
    }
}
