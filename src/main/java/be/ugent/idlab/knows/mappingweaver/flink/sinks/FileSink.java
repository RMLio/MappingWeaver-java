package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.TargetSink;
import org.jspecify.annotations.Nullable;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes triples into a specified file
 *
 * Warning: this class is IO-heavy, as it opens and closes a file at every writeback. For efficiency, use AggregateFileSink
 */
public class FileSink implements TargetSink<String> {

    private final String filePath;

    public FileSink(String filePath)  {
        this.filePath = filePath;
    }

    @Override
    public void sink(@Nullable String serializedOutput) {
        if (serializedOutput != null) {
            try (FileWriter writer = new FileWriter(this.filePath)) {
                writer.write(serializedOutput);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
