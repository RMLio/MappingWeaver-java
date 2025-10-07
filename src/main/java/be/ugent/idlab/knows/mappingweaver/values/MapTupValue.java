package be.ugent.idlab.knows.mappingweaver.values;

import org.jspecify.annotations.Nullable;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;

public class MapTupValue extends AMLValue<@Nullable MappingTuple> {

    public MapTupValue(MappingTuple value) {
        super(value);
    }

    public MapTupValue() {
        super();
    }
}
