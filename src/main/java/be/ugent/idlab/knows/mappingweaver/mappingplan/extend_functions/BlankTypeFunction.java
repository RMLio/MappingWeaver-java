
package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.CollectionNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
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

    /**
     * Every value the inner function produces, so that a function producing several of
     * them (a split, for instance) labels a blank node with every one.
     */
    @Override
    public List<String> applyMulti(@Nullable SolutionMapping solutionMapping) {
        return this.innerFunction.applyMulti(solutionMapping);
    }

    /**
     * The blank nodes as a single node: several of them are held together in a
     * {@link CollectionNode}, so that a term is generated per member where the collection
     * is serialized, the way a multi-valued literal or IRI object map does.
     */
    @Override
    public List<RDFNode> applyMultiToNode(@Nullable SolutionMapping solutionMapping) {
        // the nodes are built from the values read here, and the inner function is not
        // asked a second time: a function generating a label counts every call, so asking
        // twice would consume two labels per record
        List<String> values = applyMulti(solutionMapping);
        if (values.isEmpty()) {
            return List.of();
        }

        List<RDFNode> blankNodes = values.stream().map(RDFType.Blank::create).toList();

        return blankNodes.size() == 1 ? blankNodes : List.of(new CollectionNode(blankNodes));
    }

}
