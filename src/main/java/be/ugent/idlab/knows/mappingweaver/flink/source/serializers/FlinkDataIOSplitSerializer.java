package be.ugent.idlab.knows.mappingweaver.flink.source.serializers;

import java.io.ObjectInputStream;

import be.ugent.idlab.knows.mappingweaver.flink.source.dataio.FlinkDataIOSplit;

public class FlinkDataIOSplitSerializer implements FlinkDataIOSerializer<FlinkDataIOSplit> {

    @Override
    public FlinkDataIOSplit readObjectInputStream(ObjectInputStream bis) throws Exception {
        try {
            return (FlinkDataIOSplit) bis.readObject();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
