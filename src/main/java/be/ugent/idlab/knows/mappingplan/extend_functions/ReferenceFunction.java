package be.ugent.idlab.knows.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.exceptions.MappingException;
import org.jspecify.annotations.Nullable;

/**
 * ExtendFunction that performs a reference on the passed SolutionMapping
 *
 * @param referenceAttribute a String containing the JSON description of the
 *                           inner function
 */
public record ReferenceFunction(String referenceAttribute) implements ExtendFunction {
    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        if (solutionMapping == null) {
            return null;
        }

        if (solutionMapping.containsKey(this.referenceAttribute) || this.referenceAttribute.endsWith(".#")) {
            RDFNode value = solutionMapping.get(this.referenceAttribute);
            if (value == null || value.isNull()) {
                return null;
            }

            if (!value.toString().isEmpty()) {
                return value.getValue().toString();
            }
        }
        throw new MappingException("Specified reference attribute '" + this.referenceAttribute + "' not present in the input data");
    }

    public RDFNode applyToNode(@Nullable SolutionMapping solutionMapping) {
        if (solutionMapping == null) {
            return null;
        }
        if (solutionMapping.containsKey(this.referenceAttribute)) {
            return solutionMapping.get(this.referenceAttribute);
        }

        throw new MappingException("Specified reference attribute '" + this.referenceAttribute + "' not present in the input data");
    }

}
