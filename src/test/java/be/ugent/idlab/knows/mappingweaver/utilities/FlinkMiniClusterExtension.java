package be.ugent.idlab.knows.mappingweaver.utilities;

import org.apache.flink.runtime.testutils.MiniClusterResource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit5 extension to reuse one Minicluster across the entire test run.
 * <a href="https://stackoverflow.com/a/73312086">Implementation based on this answer by David Anderson</a>
 */
public class FlinkMiniClusterExtension implements BeforeAllCallback {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkMiniClusterExtension.class);
    private static final int PARALLEL_TESTS = 4;
    private static final int SLOTS_PER_TEST = 2;
    private static final ExtensionContext.Namespace NAMESPACE =
            ExtensionContext.Namespace.create(FlinkMiniClusterExtension.class);

    @Override
    public void beforeAll(ExtensionContext context) {
        context.getRoot().getStore(NAMESPACE).getOrComputeIfAbsent(
                FlinkMiniClusterExtension.class,
                ignored -> new SharedCluster(),
                SharedCluster.class);
    }

    private static final class SharedCluster implements ExtensionContext.Store.CloseableResource {
        private final MiniClusterResource flinkCluster;

        private SharedCluster() {
            flinkCluster = new MiniClusterResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(PARALLEL_TESTS * SLOTS_PER_TEST)
                            .setNumberTaskManagers(1)
                            .build());
            try {
                flinkCluster.before();
            } catch (Exception exception) {
                throw new ExtensionConfigurationException("Could not start the shared Flink MiniCluster", exception);
            }
            LOG.info("Shared Flink MiniCluster Web UI: {}", flinkCluster.getRestAddress());
        }

        @Override
        public void close() throws Exception {
            flinkCluster.after();
        }
    }
}
