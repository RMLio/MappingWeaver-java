package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.List;

public record NopFunction() implements ExtendFunction {
    private static final List<RDFNode> nopResult = List.of(new LiteralNode(""));
    @Override
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        return nopResult;
    }
}
