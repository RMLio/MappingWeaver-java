package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

public record ConcatenateFunction(ExtendFunction leftFunc, ExtendFunction rightFunc,
                                  String separator) implements ExtendFunction {
    @Override
    public @Nullable List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        List<RDFNode> left = leftFunc.apply(solutionMapping);
        if (left == null) {
            return null;
        }
        List<RDFNode> right = rightFunc.apply(solutionMapping);
        if (right == null) {
            return null;
        }
        List<RDFNode> result = new ArrayList<>();
        for (RDFNode leftNode : left) {
            for (RDFNode rightNode : right) {
                result.add(new LiteralNode(leftNode.getValue().toString() + separator + rightNode.getValue().toString()));
            }
        }
        return result;
    }
}
