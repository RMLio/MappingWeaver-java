
package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.BlankNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public record BlankTypeFunction(ExtendFunction innerFunction) implements ExtendFunction {

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Blank);
    }

    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        List<RDFNode> innerNodes = innerFunction.apply(solutionMapping);
        if (innerNodes == null) {
            return null;
        }
        List<BlankNode> blankNodes = innerNodes.stream().map(node -> new BlankNode(node.getValue().toString())).toList();
        return new ArrayList<>(blankNodes);
    }
}
