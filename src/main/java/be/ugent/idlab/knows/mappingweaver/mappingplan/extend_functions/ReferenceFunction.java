package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * ExtendFunction that performs a reference on the passed SolutionMapping
 *
 * @param referenceAttribute a String containing the JSON description of the
 *                           inner function
 */
public record ReferenceFunction(String referenceAttribute) implements ExtendFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ReferenceFunction.class);

    /**
     * The node the record holds for this reference, or {@code null} if it holds none.
     * <p>
     * A reference to something the record does not have is NULL rather than an error, as
     * the RML-IO registry requires of a JSONPath referring to a non-existent name or
     * child: the term that would have used the value is simply not generated. An empty
     * value is a value, and is returned as it is.
     */
    @Override
    @Nullable
    public RDFNode applyToNode(@Nullable SolutionMapping solutionMapping) {
        if (solutionMapping == null) {
            return null;
        }

        RDFNode value = solutionMapping.get(this.referenceAttribute);
        if (value == null || value.isNull()) {
            LOG.debug("Reference '{}' has no value in this record, so no term is generated for it. "
                    + "The record holds: {}", this.referenceAttribute, solutionMapping.keySet());
            return null;
        }

        return value;
    }

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        RDFNode value = applyToNode(solutionMapping);

        return value == null ? null : value.getValue().toString();
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
}
