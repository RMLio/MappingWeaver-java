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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.jspecify.annotations.Nullable;

import be.ugent.idlab.knows.amo.functions.TargetSink;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.STDSink;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import picocli.CommandLine;
import picocli.CommandLine.MissingParameterException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.UsageMessageSpec;
import picocli.CommandLine.ParseResult;


public class Main {
    private final List<String> subcommands = List.of("toFile", "toKafka", "toMQTT", "toTCPSocket", "toWebSocket", "noOutput");
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
                TargetSink<String> newSink = new CommonSink();
                for (int i = 0; i < p.getOperatorGraph().getOperators().size(); i++) {
                    Operator op = p.getOperatorGraph().getOperators().get(i);
                    if (op instanceof TargetOperator targetOperator && targetOperator.getSink() instanceof STDSink) {
                        TargetOperator newOp = new TargetOperator(targetOperator.getOperatorName(), targetOperator.getInputFragments(), targetOperator.getTargetVariable(), newSink);
                        p.getOperatorGraph().getOperators().set(i, newOp);
                    }
                }
            }

            Runnable afterPlan = null;

            // process the commands
            if (options.hasSubcommand()) {
                ParseResult subcommand = options.subcommand();

                afterPlan = switch (subcommand.commandSpec().name()) {
                    case "toFile" -> handleToFile(subcommand);
                    case "toKafka" -> handleToKafka(subcommand);
                    case "toTCPSocket" -> handleTCPSocket(subcommand);
                    case "toMQTT" -> handleToMQTT(subcommand);
                    case "toWebSocket" -> handleToWebSocket(subcommand);
                    case "noOutput" -> null; // no-op
                    default ->
                            throw new IllegalArgumentException("Invalid subcommand: " + subcommand.commandSpec().name());
                };
            }

            String jobname = options.matchedOptionValue("-j", null);
            p.execute(jobname, context);

            if (afterPlan != null) {
                afterPlan.run();
            }
        } catch (MissingParameterException e) {
            commandLine.usage(System.out);
            System.exit(1);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Runnable handleToKafka(ParseResult subcommand) throws IOException {
        String brokers = subcommand.matchedOptionValue("-b", null);

        if (brokers == null) {
            throw new IllegalArgumentException("Invalid subcommand: " + subcommand.commandSpec().name());
        }

        Properties props = new Properties();
        props.putAll(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));

        String topic = subcommand.matchedOptionValue("-t", null);
        Integer partitionId = subcommand.matchedOptionValue("--partition-id", null);

        return () -> {
            System.out.printf("Writing back to broker " + brokers + ", topic " + topic);
            if (partitionId != null) {
                System.out.println(", partition " + partitionId);
            } else {
                System.out.println();
            }
            try (Producer<String, String> producer = new KafkaProducer<>(props)) {

                if (this.doOutputBulk) {
                    producer.send(new ProducerRecord<>(topic, partitionId, null, CommonSink.getBulkOutput())).get();
                } else {
                    for (String serializedOutput : CommonSink.output) {
                        producer.send(new ProducerRecord<>(topic, partitionId, null, serializedOutput.toString())).get();
                    }
                }

                producer.flush();
            } catch (ExecutionException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable handleToFile(ParseResult subcommand) throws IOException {
        String outputFile = subcommand.matchedOptionValue("-o", null);
        if (outputFile != null) {
            return () -> {
                try (FileWriter fw = new FileWriter(outputFile)) {

                    if (this.doOutputBulk) {
                        fw.write(CommonSink.getBulkOutput());
                    } else {
                        for (String serializedOutput : CommonSink.output) {
                            fw.write(serializedOutput + "\n");
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
        } else {
            throw new IllegalArgumentException("No output file specified");
        }
    }

    private Runnable handleTCPSocket(ParseResult subcommand) throws IOException {
        String outputSocket = subcommand.matchedOptionValue("-s", null);

        String host = outputSocket.split(":")[0];
        int port = Integer.parseInt(outputSocket.split(":")[1]);
        return () -> {
            try (Socket socket = new Socket(host, port)) {
                PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);

                if (this.doOutputBulk) {
                    writer.println(CommonSink.getBulkOutput());
                } else {
                    for (String serializedOutput : CommonSink.output) {
                        writer.println(serializedOutput);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable handleToMQTT(ParseResult subcommand) throws IOException {
        String broker = subcommand.matchedOptionValue("-b", null);
        if (broker == null) throw new IllegalArgumentException("Missing option -b for subcommand toMQTT");

        String topic = subcommand.matchedOptionValue("-t", null);
        if (topic == null) throw new IllegalArgumentException("Missing option -t for subcommand toMQTT");

        return () -> {
            try {
                MqttClient client = new MqttClient("tcp://" + broker, "AML-output-sink");
                client.connect();

                if (this.doOutputBulk) {
                    client.publish(topic, new MqttMessage(CommonSink.getBulkOutput().getBytes()));
                } else {
                    for (String serializedOutput : CommonSink.output) {
                        client.publish(topic, new MqttMessage(serializedOutput.getBytes()));
                    }
                }

                client.disconnect();
                client.close();
            } catch (MqttException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private Runnable handleToWebSocket(ParseResult subcommand) throws IOException {
        String url = subcommand.matchedOptionValue("-u", null);
        if (url == null) throw new IllegalArgumentException("Missing option -u for subcommand toWebSocket");

        return () -> {
            try {
                HttpClient client = HttpClient.newHttpClient();
                var builder = client.newWebSocketBuilder();
                WebSocket webSocket = builder.buildAsync(URI.create(url), new WebSocket.Listener() {
                    @Override
                    public void onOpen(WebSocket ws) {
                        WebSocket.Listener.super.onOpen(ws);
                    }

                    @Override
                    public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public void onError(WebSocket ws, Throwable error) {
                        System.err.println("WebSocket error: " + error.getMessage());
                    }
                }).join();

                if (this.doOutputBulk) {
                    webSocket.sendText(CommonSink.getBulkOutput(), true).join();
                } else {
                    for (String serializedOutput : CommonSink.output) {
                        webSocket.sendText(serializedOutput, true).join();
                    }
                }

                webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "done").join();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
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
