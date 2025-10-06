package be.ugent.idlab.knows.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

public record NopFunction() implements ExtendFunction {
    @Override
    public @Nullable String apply(@Nullable SolutionMapping solutionMapping) {
        return "";
    }
}
