package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import org.apache.flink.api.common.functions.MapFunction;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;

public class SolMapValueToStringExtractor implements MapFunction<MapTupValue, String> {

    private String targetVariable;

    public SolMapValueToStringExtractor(String targetVariable) {
        this.targetVariable = targetVariable;
    }

    @Override
    public String map(MapTupValue input) throws Exception {
        MappingTuple tuple = input.getValue();
        StringBuilder builder  = new StringBuilder(""); 
        if (tuple != null) {
            for (String fragment : tuple.getFragments()) {
                for (SolutionMapping mapping : tuple.getSolutionMappings(fragment)) {
                    if (mapping != null) {
                        RDFNode node = mapping.get(this.targetVariable);
                        if (node != null && !node.isNull()){
                            LiteralNode literal =  (LiteralNode) node;
                            builder = builder.append(literal.getValue() + "\n");
                        }
                    }
                }

            }
        }
        return builder.toString(); 
    }
}
