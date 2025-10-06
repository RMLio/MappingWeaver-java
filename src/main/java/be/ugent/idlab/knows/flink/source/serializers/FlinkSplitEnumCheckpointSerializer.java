
package be.ugent.idlab.knows.flink.source.serializers;

import java.io.ObjectInputStream;

import be.ugent.idlab.knows.flink.source.dataio.FlinkSplitEnumeratorCheckpoint;

public class FlinkSplitEnumCheckpointSerializer implements FlinkDataIOSerializer<FlinkSplitEnumeratorCheckpoint>{

    @Override
    public FlinkSplitEnumeratorCheckpoint readObjectInputStream(ObjectInputStream in) throws Exception {
        return (FlinkSplitEnumeratorCheckpoint) in.readObject();
    }

}
