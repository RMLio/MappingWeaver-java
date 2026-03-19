package be.ugent.idlab.knows.mappingweaver.mappingplan;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.SerializeOperator;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import be.ugent.idlab.knows.mappingweaver.values.MapTupValue;

/**
 * Mapping plan for execution on the input files
 */
public class MappingPlan {

    public static final String CONFIG_WATERMARK_INTERVAL = "watermark-interval";
    public static final String CONFIG_LOCAL_PARALLEL = "local-parallel";

    private final OperatorGraph operatorGraph;
    private final StreamExecutionEnvironment env;
    private GraphOpVisitor visitor;
    private boolean isInitialized;

    /**
     * @param env     environment to execute the operators on
     * @param root    root of the operator graph
     * @param visitor visitor to chain the operators together
     */
    public MappingPlan(StreamExecutionEnvironment env, OperatorGraph root, GraphOpVisitor visitor) {
        this.env = env;
        this.operatorGraph = root;
        this.visitor = visitor;
        this.isInitialized = false;
    }

    /**
     * Constructs a mapping plan based on the JSON description found in the file
     * The JSON description should be as specified by the AlgeMapLoom-rs. See
     * test/resources directory for examples
     *
     * @param path path to the JSON file
     * @return a MappingPlan representing the instance
     */
    public static MappingPlan fromFile(StreamExecutionEnvironment env, String path, String defaultBaseIRI) throws IOException {
        return JSONPlanParser.fromFile(env, path, defaultBaseIRI);
    }

    public static MappingPlan fromString(StreamExecutionEnvironment env, String json, String basePath, String defaultBaseIRI) {
        return JSONPlanParser.fromString(env, json, basePath, defaultBaseIRI);
    }

    public JobExecutionResult execute(String jobname, Map<String, Object> extraOptions) throws Exception {
        if (jobname == null) {
            jobname = "Flink-MappingJob-" + new Random().nextInt(Integer.MAX_VALUE);
        }

        if (extraOptions.containsKey(CONFIG_WATERMARK_INTERVAL)) {
            this.visitor.setWatermarkInterval((Long) extraOptions.get(CONFIG_WATERMARK_INTERVAL));
        }

        if (extraOptions.containsKey(CONFIG_LOCAL_PARALLEL)) {
            this.visitor.setLocalParallel((Boolean) extraOptions.get(CONFIG_LOCAL_PARALLEL));
        }

        initializeFlinkTopology(true);

        return this.env.execute(jobname);
    }

    public void initializeFlinkTopology(boolean includeTarget) {
        if (!this.isInitialized) {
            List<Operator> order = this.operatorGraph.topologicalOrder();
            for (Operator operator : order) {
                if (includeTarget || !(operator instanceof TargetOperator)) {
                    operator.accept(this.visitor);
                }
            }
        }
        this.isInitialized = true;
    }

    public List<DataStream<MapTupValue>> getSerializedDataStreams() {
        Map<Operator, DataStream<MapTupValue>> cache = this.visitor.getStreamCache();
        return cache.keySet().stream()
            .filter((op) -> op != null && op.getOperatorName().toLowerCase().contains("serialize"))
            .map(cache::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    public JobExecutionResult execute() throws Exception {
        Random r = new Random();
        String jobname = "Flink-MappingJob-" + r.nextInt(Integer.MAX_VALUE);
        return this.execute(jobname, Map.of());
    }

    public void setVisitor(GraphOpVisitor visitor) {
        this.visitor = visitor;
    }

    public OperatorGraph getOperatorGraph() {
        return operatorGraph;
    }

}
