package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

public class GenerateBlankNode implements ExtendFunction {
    private final AtomicLong counter = new AtomicLong(1);

    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Blank);
    }
    @Override
    public @Nullable String apply(@Nullable SolutionMapping mapping) {
        long nr = counter.getAndIncrement();
        return "Blank" + nr;
    }
}
