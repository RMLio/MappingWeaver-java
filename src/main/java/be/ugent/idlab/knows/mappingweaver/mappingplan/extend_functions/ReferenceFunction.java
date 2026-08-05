package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.Optional;

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

        RDFNode value = solutionMapping.get(this.referenceAttribute);

        // A reference to something the record does not have yields NULL rather than an
        // error: the RML-IO registry requires it of a JSONPath referring to a non-existent
        // name or child, and the term that would have used the value is simply not
        // generated. An empty value is a value, and is returned as it is.
        if (value == null || value.isNull()) {
            return null;
        }

        return value.getValue().toString();
    }

    /**
     * A reference reads an attribute of the record as-is, so a source field carrying this
     * function reads the attribute straight from the record: only then can a path that
     * matches several values (a JSON array, an XML node list) yield all of them.
     */
    @Override
    public Optional<String> asReference() {
        return Optional.of(this.referenceAttribute);
    }

    @Nullable
    public RDFNode applyToNode(@Nullable SolutionMapping solutionMapping) {
        if (solutionMapping == null) {
            return null;
        }

        // as above: absent is NULL, not an error
        return solutionMapping.get(this.referenceAttribute);
    }

}
