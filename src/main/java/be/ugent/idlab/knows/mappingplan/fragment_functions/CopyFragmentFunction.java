package be.ugent.idlab.knows.mappingplan.fragment_functions;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.functions.FragmentFunction;

import java.util.Collection;

import org.jspecify.annotations.Nullable;

public record CopyFragmentFunction(Collection<String> from, Collection<String> to) implements FragmentFunction {
    @Override
    @Nullable 
    public MappingTuple apply(@Nullable MappingTuple mappingTuple) {
        if (mappingTuple == null){
            return null; 
        }

        MappingTuple out = new MappingTuple();
        for (String outputFragment : this.to) {
            for (String inputFragment : from) {
                out.setSolutionMaps(outputFragment, mappingTuple.getSolutionMappings(inputFragment));
            }
        }
        return out;
    }
}
