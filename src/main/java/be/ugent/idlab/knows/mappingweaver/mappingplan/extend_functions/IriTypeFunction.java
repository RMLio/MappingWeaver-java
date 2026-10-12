package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.IRINode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.apache.jena.irix.IRIException;
import org.apache.jena.irix.IRIx;
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

    public IriTypeFunction(String baseIri, ExtendFunction innerFunction) {

        this.innerFunction = innerFunction;
        this.baseIri = baseIri;
        this.useBaseIri = !(innerFunction instanceof ConstantValueFunction);
    }

    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping arg0) {
        return asIri(this.innerFunction.apply(arg0));
    }

    /**
     * Turns a value the inner function produced into an IRI. A value that is an absolute IRI
     * (one with a scheme, any scheme, and optionally a fragment) is used as it is; otherwise the base IRI is prepended, and a
     * result that is not a valid absolute IRI generates no term.
     */
    @Nullable
    private List<RDFNode> asIri(@Nullable List<RDFNode> result) {
        if (result != null) {
            List<IRINode> iris = result.stream()
                    .map(node -> {
                        String iriValue = node.getValue().toString();
                        if (this.useBaseIri && !isAbsoluteIri(iriValue)) {
                            String prepended = this.baseIri + iriValue;
                            if (!isAbsoluteIri(prepended)) {
                                log.warn("System was unable to generate a valid IRI with %s, bailing out.".formatted(iriValue));
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

    private static boolean isAbsoluteIri(String value) {
        try {
            return IRIx.create(value).isReference();
        } catch (IRIException e) {
            return false;
        }
    }

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.IRI);
    }

}
