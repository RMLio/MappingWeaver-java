package be.ugent.idlab.knows.mappingweaver.cli;

import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.UsageMessageSpec;

import java.util.List;

public final class CliCommand {

    public static CommandSpec create() {
        CommandSpec toFile = CommandSpec.create()
                .name("toFile")
                .helpCommand(true)
                .usageMessage(new UsageMessageSpec().description("Write output to file"))
                .addOption(OptionSpec.builder("-o", "--output-path")
                        .description("The path to an output file. Note: when a StreamingFileSink is used (the mapping consists only of stream triple maps), this path specifies a directory and optionally an extension. Part files will be written to the given directory and the given extension will be used for each part file.")
                        .paramLabel("<output file>")
                        .type(String.class)
                        .required(true)
                        .help(true)
                        .build());

        CommandSpec toKafka = CommandSpec.create()
                .name("toKafka")
                .usageMessage(new UsageMessageSpec()
                        .description("Write output to a Kafka topic")
                )
                .helpCommand(true)
                .addOption(OptionSpec.builder("-b", "--broker-list")
                        .type(String.class)
                        .required(true)
                        .paramLabel("<host:port>")
                        .build())
                .addOption(OptionSpec.builder("-t", "--topic")
                        .paramLabel("topic name")
                        .type(String.class)
                        .required(true)
                        .description("The name of the Kafka topic to write output to.")
                        .build())
                .addOption(OptionSpec.builder("--partition-id")
                        .description("EXPERIMENTAL. The partition id of kafka topic to which the output will be written to.")
                        .type(Integer.class)
                        .paramLabel("<id>")
                        .build());

        CommandSpec toTCPSocket = CommandSpec.create()
                .name("toTCPSocket")
                .addOption(OptionSpec.builder("-s")
                        .required(true)
                        .paramLabel("<host:port>")
                        .type(String.class)
                        .build())
                .usageMessage(new UsageMessageSpec().description("Write output to a TCP socket"));

        CommandSpec toMQTT = CommandSpec.create()
                .name("toMQTT")
                .addOption(OptionSpec.builder("-b")
                        .type(String.class)
                        .paramLabel("<host:port>")
                        .required(true)
                        .build())
                .addOption(OptionSpec.builder("-t")
                        .type(String.class)
                        .paramLabel("<topic>")
                        .required(true)
                        .build())
                .usageMessage(new UsageMessageSpec().description("Write output to an MQTT topic"));

        CommandSpec toWebSocket = CommandSpec.create()
                .name("toWebSocket")
                .addOption(OptionSpec.builder("-u", "--url")
                        .type(String.class)
                        .required(true)
                        .paramLabel("<ws://host:port/path>")
                        .description("The WebSocket URL to send output to (ws:// or wss://)")
                        .build())
                .usageMessage(new UsageMessageSpec().description("Write output to a WebSocket endpoint"));

        CommandSpec noOutput = CommandSpec.create()
                .name("noOutput")
                .usageMessage(new UsageMessageSpec().description("Do everything, but discard output"));


        CommandSpec root = CommandSpec.create()
                .mixinStandardHelpOptions(true)
                .exitCodeOnInvalidInput(1)
                .name("AlgeMapLoom")
//                .usageMessage(new UsageMessageSpec().description("Usage: RMLStreamer [toFile|toKafka|toTCPSocket|toMQTT|noOutput] [options]"))
                .addSubcommand("toFile", toFile)
                .addSubcommand("toKafka", toKafka)
                .addSubcommand("toTCPSocket", toTCPSocket)
                .addSubcommand("toMQTT", toMQTT)
                .addSubcommand("toWebSocket", toWebSocket)
                .addSubcommand("noOutput", noOutput)
                .addOption(OptionSpec.builder("-j", "--job-name")
                        .paramLabel("<job name>")
                        .type(String.class)
                        .description("The name to assign to the job on the Flink cluster. Put some semantics in here ;)")
                        .build())
                .addOption(OptionSpec.builder("-i", "--base-iri")
                        .description("The base IRI as defined in the R2RML spec.")
                        .paramLabel("<base IRI>")
                        .type(String.class)
                        .build())
                .addOption(OptionSpec.builder("--disable-local-parallel")
                        .description("By default input records are spread over the available task slots within a task manager to optimise parallel processing, at the cost of losing the order of the records throughout the process. This option disables this behaviour to guarantee that the output order is the same as the input order.")
                        .build())
                .addOption(OptionSpec.builder("-p", "parallelism")
                        .paramLabel("<task slots>")
                        .type(Integer.class)
                        .description("Sets the maximum operator parallelism (~nr of task slots used)")
                        .build())
                .addOption(OptionSpec.builder("-m", "--mapping-file")
                        .paramLabel("<RML mapping file>")
                        .type(String.class)
                        .required(true)
                        .description("The path to an RML mapping file. The path must be accessible on the Flink cluster.")
                        .build())
                .addOption(OptionSpec.builder("--json-ld")
                        .description("Write the output as JSON-LD instead of N-Quads. An object contains all RDF generated from one input record. Note: this is slower than using the default N-Quads format.")
                        .build())
                .addOption(OptionSpec.builder("--bulk")
                        .description("Write all triples generated from one input record at once, instead of writing triples the moment they are generated.")
                        .build())
                .addOption(OptionSpec.builder("--checkpoint-interval")
                        .description("If given, Flink's checkpointing is enabled with the given interval. If not given, checkpointing is enabled when writing to a file (this is required to use the flink StreamingFileSink). Otherwise, checkpointing is disabled.\n")
                        .paramLabel("<time (ms)>")
                        .type(Long.class)
                        .build())
                .addOption(OptionSpec.builder("--auto-watermark-interval")
                        .description("If given, Flink's watermarking will be generated periodically with the given interval. If not given, a default value of 50ms will be used.This option is only valid for DataStreams.")
                        .paramLabel("<time (ms)>")
                        .type(Long.class)
                        .defaultValue("50")
                        .build())
                .addOption(OptionSpec.builder("-f", "--function-descriptions")
                        .paramLabel("<function descriptions>")
                        .type(List.class).auxiliaryTypes(String.class) // List<String>
                        .description("An optional comma-separated list of paths to function description files (in RDF using FnO). A path can be a file location or a URL.")
                        .build());
        return root;
    }
}