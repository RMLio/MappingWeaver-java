package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.apache.commons.validator.routines.UrlValidator;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public String apply(@Nullable SolutionMapping arg0) {
        String result = this.innerFunction.apply(arg0);
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
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.IRI);
    }

}
