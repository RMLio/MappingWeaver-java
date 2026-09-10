package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.IRINode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * ExtendFunction that returns an IRI node containing the URL, as specified by
 * the inner function
 *
 * @param uriEncodeInnerFuncJson a String with the JSON description of the inner
 *                               function
 */
public record EncodeUriFunction(ExtendFunction uriEncodeInnerFuncJson) implements ExtendFunction {

    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        List<RDFNode> innerValues = this.uriEncodeInnerFuncJson.apply(solutionMapping);
        if (innerValues == null) {
            return null;
        }
        return new ArrayList<>(innerValues.stream().map(innerNode -> {
            final String innerValue = innerNode.getValue().toString();
            return new IRINode(URLEncoder.encode(innerValue, StandardCharsets.UTF_8));
        }).toList());

    }
}
