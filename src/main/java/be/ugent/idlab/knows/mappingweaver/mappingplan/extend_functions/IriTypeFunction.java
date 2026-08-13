package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.validator.routines.UrlValidator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.CollectionNode;
import be.ugent.idlab.knows.amo.blocks.nodes.IRINode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;

public class IriTypeFunction implements ExtendFunction {
    private static final Logger log = LoggerFactory.getLogger(IriTypeFunction.class);
    private final ExtendFunction innerFunction;
    private final String baseIri;
    private final boolean useBaseIri;
    private final UrlValidator validator;

    public IriTypeFunction(String baseIri, ExtendFunction innerFunction) {

        this.innerFunction = innerFunction;
        this.baseIri = baseIri;
        this.useBaseIri = !(innerFunction instanceof ConstantValueFunction);

        this.validator = UrlValidator.getInstance();

    }

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping arg0) {
        return asIri(this.innerFunction.apply(arg0));
    }

    /**
     * Every IRI this function stands for. A function producing several values, a split for
     * instance, gives an IRI per value; a value no valid IRI can be built from is left out.
     */
    @Override
    public List<String> applyMulti(@Nullable SolutionMapping arg0) {
        return this.innerFunction.applyMulti(arg0).stream()
                .map(this::asIri)
                .filter(Objects::nonNull)
                .toList();
    }

    /**
     * Turns a value the inner function produced into an IRI, prepending the base IRI when
     * the value is not one already.
     */
    @Nullable
    private String asIri(@Nullable String result) {
        if (result != null && this.useBaseIri) {
            if (!this.validator.isValid(result)) {
                String prepended = this.baseIri + result;
                if (!this.validator.isValid(prepended)) {
                    log.warn("System was unable to generate a valid URL with %s, bailing out.".formatted(result));
                    return null;
                }

                return prepended;
            }
        }

        return result;
    }

    @Override
    @Nullable
    public RDFNode applyToNode(@Nullable SolutionMapping arg0) {
        List<RDFNode> nodes = applyMultiToNode(arg0);

        return nodes.isEmpty() ? null : nodes.get(0);
    }

    /**
     * The IRIs as a single node: several of them are held together in a
     * {@link CollectionNode}, so that a term is generated per member where the collection
     * is serialized, the way a multi-valued literal object map does.
     */
    @Override
    public List<RDFNode> applyMultiToNode(@Nullable SolutionMapping arg0) {
        List<RDFNode> iris = applyMulti(arg0).stream()
                .map(iri -> (RDFNode) new IRINode(iri))
                .toList();

        if (iris.size() <= 1) {
            return iris;
        }

        return List.of(new CollectionNode(iris));
    }

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.IRI);
    }

}
