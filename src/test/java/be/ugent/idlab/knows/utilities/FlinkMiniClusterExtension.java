package be.ugent.idlab.knows.utilities;

import com.esotericsoftware.minlog.Log;
import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit5 extension to reuse the Minicluster in tests.
 * <a href="https://stackoverflow.com/a/73312086">Implementation based on this answer by David Anderson</a>
 */
public class FlinkMiniClusterExtension implements BeforeEachCallback,  AfterEachCallback {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterExtension.class);
    private static final int PARALLELISM = 2;
    private static MiniClusterResource flinkCluster;

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        flinkCluster = new MiniClusterResource(
                new MiniClusterResourceConfiguration.Builder()
                        .setNumberSlotsPerTaskManager(PARALLELISM)
                        .setNumberTaskManagers(1)
                        .build());
        flinkCluster.before();

        Log.info("Web UI: " + flinkCluster.getRestAddress());
    }


    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        flinkCluster.after(); 
    }
}
