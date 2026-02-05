package be.ugent.idlab.knows.mappingweaver.cli;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.json.JSONObject;
import org.jspecify.annotations.Nullable;

import be.ugent.idlab.knows.amo.functions.TargetSink;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.STDSink;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.SolMapValueToStringExtractor;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.WeaverSinkFactory;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;
import picocli.CommandLine;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParseResult;

public class Main {
    private final List<String> subcommands = List.of("toFile", "toKafka", "toMQTT", "toTCPSocket", "toWebSocket",
            "noOutput");
    private boolean doOutputBulk = false;

    public static void main(String[] args) {
        new Main().parseAndRun(args);
    }

    private void parseAndRun(String[] args) {
        CommonSink.output.clear();
        CommandSpec root = CliCommand.create();

        CommandLine commandLine = new CommandLine(root);

        ParseResult options = commandLine.parseArgs(args);

        try {
            String mappingFile = options.matchedOptionValue("-m", "");
            if (mappingFile.isEmpty()) {
                commandLine.usage(System.out);
                System.exit(1);
            }
            // read in the mapping file and generate a mapping plan
            Path path = Paths.get(mappingFile);
            String document = Files.readString(path);

            // check for base iri
            if (options.hasMatchedOption("-i")) {
                String baseIRI = options.matchedOptionValue("-i", "null");
                // go through the lines and replace the @base entry with the option provided
                document = document.lines()
                        .map(l -> l.startsWith("@base") ? "@base <" + baseIRI + "> ." : l)
                        .reduce("", (s1, s2) -> s1 + "\n" + s2).strip();
            }

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            Map<String, Object> context = new HashMap<>();

            if (options.hasMatchedOption("-p")) {
                int parallelism = Integer.parseInt(options.matchedOptionValue("-p", "1"));

                if (parallelism < 1) {
                    parallelism = 1;
                }
                env.setParallelism(parallelism);
            }

            this.doOutputBulk = options.matchedOptionValue("--bulk", "false").equalsIgnoreCase("true");

            if (options.hasMatchedOption("--disable-local-parallel")) {
                context.put(MappingPlan.CONFIG_LOCAL_PARALLEL, false);
            }

            if (options.hasMatchedOption("--checkpoint-interval")) {
                long interval = options.matchedOptionValue("--checkpoint-interval", null);
                env.enableCheckpointing(interval);
            }

            if (options.hasMatchedOption("--auto-watermark-interval")) {
                long interval = options.matchedOptionValue("--auto-watermark-interval", null);
                context.put(MappingPlan.CONFIG_WATERMARK_INTERVAL, interval);
            }

            if (options.hasMatchedOption("--function-descriptions")) {
                List<String> descriptions = options.matchedOptionValue("--function-descriptions", List.of());
                context.put("function-descriptions", descriptions);
            }

            ITranslator t = ITranslator.getInstance();
            String jsonPlan = t.translate_to_document(document);

            String basePath;
            if (path.getParent() == null) {
                basePath = System.getProperty("user.dir");
            } else {
                basePath = path.getParent().toString() + '/';
            }

            MappingPlan p = JSONPlanParser.fromString(env, jsonPlan, basePath);

            if (options.hasSubcommand() && this.subcommands.contains(options.subcommand().commandSpec().name())) {
                p.initializeFlinkTopology(false);
                List<DataStream<MapTupValue>> serializedDataStreams = p.getSerializedDataStreams();
                DataStream<MapTupValue> firstStream = serializedDataStreams.removeFirst();
                DataStream<MapTupValue> unionStream = firstStream
                        .union(serializedDataStreams.toArray(new DataStream[0]));

                ParseResult subcommand = options.subcommand();
                switch (subcommand.commandSpec().name()) {
                    case "toFile" -> handleToFile(subcommand, unionStream);
                    default ->
                        throw new IllegalArgumentException("Invalid subcommand: " + subcommand.commandSpec().name());
                }
                ;

            }

            String jobname = options.matchedOptionValue("-j", null);
            p.execute(jobname, context);

        } catch (MissingParameterException e) {
            commandLine.usage(System.out);
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void handleToFile(ParseResult subcommand, DataStream<MapTupValue> stream) throws IOException {
        String outputFile = subcommand.matchedOptionValue("-o", null);
        if (outputFile != null) {
            JSONObject config = new JSONObject(Map.ofEntries(
                    Map.entry("target_type", "File"),
                    Map.entry("path", outputFile)));
            WeaverSinkFactory factory = new WeaverSinkFactory(config, "FileSink", "?serialized_output");
            factory.attachSink(stream).setParallelism(1); 
        } else {
            throw new IllegalArgumentException("No output file specified");
        }
    }

    public static class CommonSink implements TargetSink<String> {
        public static final List<String> output = Collections.synchronizedList(new ArrayList<>());

        public static String getBulkOutput() {
            return String.join("\n", output);
        }

        @Override
        public void sink(@Nullable String serializedOutput) {
            output.add(serializedOutput);
        }
    }
}
