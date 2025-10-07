package be.ugent.idlab.knows.mappingweaver.mappingplan;

import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Mapping plan for execution on the input files
 */
public class MappingPlan {

    public static final String CONFIG_WATERMARK_INTERVAL = "watermark-interval";
    public static final String CONFIG_LOCAL_PARALLEL = "local-parallel";

    private final OperatorGraph operatorGraph;
    private final StreamExecutionEnvironment env;
    private GraphOpVisitor visitor;

    /**
     * @param env     environment to execute the operators on
     * @param root    root of the operator graph
     * @param visitor visitor to chain the operators together
     */
    public MappingPlan(StreamExecutionEnvironment env, OperatorGraph root, GraphOpVisitor visitor) {
        this.env = env;
        this.operatorGraph = root;
        this.visitor = visitor;
    }

    /**
     * Constructs a mapping plan based on the JSON description found in the file
     * The JSON description should be as specified by the AlgeMapLoom-rs. See test/resources directory for examples
     *
     * @param path path to the JSON file
     * @return a MappingPlan representing the instance
     */
    public static MappingPlan fromFile(StreamExecutionEnvironment env, String path) throws IOException {
        return JSONPlanParser.fromFile(env, path);
    }

    public static MappingPlan fromString(StreamExecutionEnvironment env, String json, String basePath) {
        return JSONPlanParser.fromString(env, json, basePath);
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

        List<Operator> order = this.operatorGraph.topologicalOrder();

        for (Operator operator : order) {
            operator.accept(this.visitor);
        }


        return this.env.execute(jobname);
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