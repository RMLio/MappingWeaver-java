package be.ugent.idlab.knows.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

public record ConcatenateFunction(ExtendFunction leftFunc, ExtendFunction rightFunc,
                                  String separator) implements ExtendFunction {
    @Override
    public @Nullable String apply(@Nullable SolutionMapping solutionMapping) {
        String left = leftFunc.apply(solutionMapping);
        if (left == null) {
            return null;
        }
        String right = rightFunc.apply(solutionMapping);
        if (right == null) {
            return null;
        }
        return left + separator + right;
    }
}
