package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import org.jspecify.annotations.Nullable;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.util.ArrayList;
import java.util.List;

public class AggregateFileSink implements AggregateSink<String> {

    private final List<String> writeList;
    private final int bulkLimit;
    private final String filePath;
    private transient FileWriter fileWriter;

    public AggregateFileSink(String filePath) throws IOException {
        this(filePath, -1);
    }

    public AggregateFileSink(String filePath, int bulkLimit) throws IOException {
        this.bulkLimit = bulkLimit;
        this.writeList = new ArrayList<>();
        this.filePath = filePath;
        this.bootstrap();
    }

    @Serial
    private void readObject(ObjectInputStream inputStream) throws IOException, ClassNotFoundException {
        inputStream.defaultReadObject();
        this.bootstrap();
    }

    private void bootstrap() throws IOException {
        this.fileWriter = new FileWriter(this.filePath);
    }

    /**
     * Method to write the remainder of writelist to the file and close the resource.
     * <p>
     * After calling this method, the sink should be considered closed and not usable anymore. Any nodes added to the writelist through sink() will no longer be written.
     *
     * @throws IOException  Something goes wrong while writing.
     */
    @Override
    public void writeback() throws IOException {
        for (String node : writeList) {
            this.fileWriter.write(node);
        }
        this.writeList.clear();
        this.fileWriter.close();
    }

    @Override
    public void sink(@Nullable String serializedOutput) {
        this.writeList.add(serializedOutput);

        if (this.bulkLimit > 0) {
            if (this.writeList.size() >= this.bulkLimit) {
                for (String node : this.writeList) {
                    // TODO: handle more nicely
                    try {
                        this.fileWriter.write(node);
                        this.fileWriter.flush();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                this.writeList.clear();
            }
        }
    }
}
