package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;


import org.jspecify.annotations.Nullable;

/**
 * ExtendFunction that always returns a constant value as a Literal node
 *
 * @param value value to be returned
 */
public record ConstantValueFunction(String value) implements ExtendFunction{

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        return this.value;
    }
}

