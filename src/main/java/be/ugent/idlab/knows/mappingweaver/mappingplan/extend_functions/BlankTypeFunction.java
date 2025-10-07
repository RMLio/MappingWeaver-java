
package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import java.util.Optional;

import org.jspecify.annotations.Nullable;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;

public record BlankTypeFunction(ExtendFunction innerFunction) implements ExtendFunction {

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Blank);
    }

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        return this.innerFunction.apply(solutionMapping);
    }

}
