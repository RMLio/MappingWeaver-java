package be.ugent.idlab.knows.mappingweaver.flink.sinks;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * A TargetSink that prints out the output to the standard output.
 */
public class STDSink implements Sink<String> {

    @Override
    public SinkWriter<String> createWriter(WriterInitContext arg0) throws IOException {
        return new STDSinkWriter(System.out);
    }

    class STDSinkWriter implements SinkWriter<String> {
        private PrintStream stream;

        public STDSinkWriter(PrintStream stream) {
            this.stream = stream;
        }

        public STDSinkWriter(OutputStream stream) {
            this.stream = new PrintStream(stream);
        }

        @Override
        public void close() throws Exception {
        }

        @Override
        public void flush(boolean arg0) throws IOException, InterruptedException {
            this.stream.flush();
        }

        @Override
        public void write(String input, Context arg1) throws IOException, InterruptedException {
            if (!input.isEmpty()) {
                this.stream.println(input);
            }
        }

    }
}
