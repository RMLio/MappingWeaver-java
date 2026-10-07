package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.IRINode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.apache.commons.validator.routines.UrlValidator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

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
    public List<RDFNode> apply(@Nullable SolutionMapping arg0) {
        return asIri(this.innerFunction.apply(arg0));
    }

    /**
     * Turns a value the inner function produced into an IRI, prepending the base IRI when
     * the value is not one already.
     */
    @Nullable
    private List<RDFNode> asIri(@Nullable List<RDFNode> result) {
        if (result != null) {
            List<IRINode> iris = result.stream()
                    .map(node -> {
                        String iriValue = node.getValue().toString();
                        if (!this.validator.isValid(iriValue) && this.useBaseIri) {
                            String prepended = this.baseIri + iriValue;
                            if (!this.validator.isValid(prepended)) {
                                log.warn("System was unable to generate a valid URL with %s, bailing out.".formatted(iriValue));
                                return null;
                            }
                            return new IRINode(prepended);
                        }
                        return new IRINode(iriValue);
                    })
                    .filter(Objects::nonNull)
                    .toList();
            return new ArrayList<>(iris);
        } else {
            return null;
        }
    }

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.IRI);
    }

}
