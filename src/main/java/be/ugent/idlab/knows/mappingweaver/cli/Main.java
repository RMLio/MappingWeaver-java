package be.ugent.idlab.knows.mappingweaver.cli;

import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingweaver.flink.sinks.WeaverSinkFactory;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno.FnOFunction;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.json.JSONObject;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {
    private final List<String> subcommands = List.of("toFile", "toKafka", "toMQTT", "toTCPSocket", "toWebSocket",
            "noOutput");

    public static void main(String[] args) {
        System.exit(run(args));
    }

    /**
     * Runs the CLI and returns its exit code: 0 on success and for {@code --help}, 1 on invalid arguments and on
     * any failure. {@link #main} passes it to {@link System#exit}, so the process ends even when Flink threads linger.
     */
    public static int run(String... args) {
        CommandLine commandLine = new CommandLine(CliCommand.create());
        CommandSpec spec = commandLine.getCommandSpec();

        ParseResult options;
        try {
            options = commandLine.parseArgs(args);
        } catch (ParameterException e) {
            System.err.println(e.getMessage());
            e.getCommandLine().usage(System.err);
            return spec.exitCodeOnInvalidInput();
        }
        if (CommandLine.printHelpIfRequested(options)) {
            return spec.exitCodeOnUsageHelp();
        }
        applyVerbosity(options);

        try {
            new Main().execute(options);
            return spec.exitCodeOnSuccess();
        } catch (Throwable t) {
            t.printStackTrace();
            return spec.exitCodeOnExecutionException();
        }
    }

    private void execute(ParseResult options) throws Exception {
        boolean isAlgeMapLoomPlan = !options.hasMatchedOption("-m");
        String mappingFile = options.matchedOptionValue(isAlgeMapLoomPlan ? "-l" : "-m", "");
        // read in the mapping file and generate a mapping plan
        Path path = Paths.get(mappingFile);
        String document = Files.readString(path);

        // check for base iri
        if (!isAlgeMapLoomPlan && options.hasMatchedOption("-i")) {
            // TODO: isn't this only valid for old RML? KGC RML adds a base IRI per triples map...
            String baseIRI = options.matchedOptionValue("-i", null);
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

        // configure FnO function descriptions before the plan is parsed (constructors read them)
        List<String> customDescriptions = options.hasMatchedOption("-f")
                ? options.matchedOptionValue("-f", List.of())
                : List.of();
        boolean customFunctionsOnly = options.hasMatchedOption("--custom-functions-only");
        FnOFunction.configure(customDescriptions, customFunctionsOnly);

        final String jsonPlan;
        if (isAlgeMapLoomPlan) {
            jsonPlan = document;
        } else {
            ITranslator t = ITranslator.getInstance();
            jsonPlan = t.translate_to_document(document);
        }

        final String basePath;
        if (path.getParent() == null) {
            basePath = System.getProperty("user.dir");
        } else {
            basePath = path.getParent().toString() + '/';
        }

        final String baseIRI = options.hasMatchedOption("-i")? options.matchedOptionValue("-i", null) : "";

        final boolean bestEffort = options.hasMatchedOption("--best-effort");

        MappingPlan p = JSONPlanParser.fromString(env, jsonPlan, basePath, baseIRI, bestEffort);

        if (options.hasSubcommand() && this.subcommands.contains(options.subcommand().commandSpec().name())) {
            p.initializeFlinkTopology(false);
            List<DataStream<MapTupValue>> serializedDataStreams = p.getSerializedDataStreams();
            DataStream<MapTupValue> firstStream = serializedDataStreams.removeFirst();
            DataStream<MapTupValue> unionStream = firstStream
                    .union(serializedDataStreams.toArray(new DataStream[0]));

            ParseResult subcommand = options.subcommand();
            if (subcommand.commandSpec().name().equals("toFile")) {
                handleToFile(subcommand, unionStream, env.getParallelism());
            } else {
                throw new IllegalArgumentException("Invalid subcommand: " + subcommand.commandSpec().name());
            }

        }

        String jobname = options.matchedOptionValue("-j", null);
        p.execute(jobname, context);
    }

    private void handleToFile(ParseResult subcommand, DataStream<MapTupValue> stream, int parallelism) {
        String outputFile = subcommand.matchedOptionValue("-o", null);
        JSONObject config = new JSONObject(Map.ofEntries(
                Map.entry("target_type", "File"),
                Map.entry("path", outputFile)));
        WeaverSinkFactory factory = new WeaverSinkFactory(config, "FileSink", "?serialized_output");
        factory.attachSink(stream).setParallelism(parallelism);
    }

    static void applyVerbosity(ParseResult options) {
        String level;
        if (options.hasMatchedOption("-vvv")) level = "DEBUG";
        else if (options.hasMatchedOption("-vv")) level = "INFO";
        else if (options.hasMatchedOption("-v")) level = "WARN";
        else level = "ERROR";
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("org.slf4j.simpleLogger.log.be.ugent.idlab.knows.mappingweaver", level.toLowerCase());
    }
}
