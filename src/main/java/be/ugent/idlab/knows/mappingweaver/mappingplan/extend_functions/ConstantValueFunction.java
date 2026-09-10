package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;


import org.jspecify.annotations.Nullable;

import java.util.List;

/**
 * ExtendFunction that always returns a constant value as a Literal node
 *
 */
public class ConstantValueFunction implements ExtendFunction{

    private final List<RDFNode> value;

    /**
     * Creates a new ConstantValueFunction that always returns the given value as a list of one Literal node
     * @param value the constant value to return
     */
    public ConstantValueFunction(final String value) {
        this.value = List.of(new LiteralNode(value));
    }

    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        return this.value;
    }
}

