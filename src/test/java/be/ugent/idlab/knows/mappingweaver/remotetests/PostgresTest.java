package be.ugent.idlab.knows.mappingweaver.remotetests;

import be.ugent.idlab.knows.mappingweaver.cores.DBTestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;


@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(FlinkMiniClusterExtension.class)
@Testcontainers
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class PostgresTest extends DBTestCore {

    // will be shared between test methods, i.e., one instance
    @Container
    protected static PostgreSQLContainer<?> container;

    public PostgresTest() {
        super("postgres", "", "postgres:latest");
        container = new PostgreSQLContainer<>(DockerImageName.parse(super.DOCKER_TAG))
                .withUsername(super.USERNAME)
                .withPassword(super.PASSWORD)
                .withEnv("POSTGRES_HOST_AUTH_METHOD", "trust")
                .withEnv("runID", Integer.toString(this.hashCode())) // to start a different container for each run
                .withDatabaseName("test")
                .withStartupTimeout(Duration.of(60, ChronoUnit.SECONDS));
        container.start();
    }

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                "RMLTC0000-PostgreSQL",
                "RMLTC0001a-PostgreSQL",
                "RMLTC0001b-PostgreSQL",
                "RMLTC0002a-PostgreSQL",
                "RMLTC0002b-PostgreSQL",
                "RMLTC0003c-PostgreSQL",
                "RMLTC0004a-PostgreSQL",
                "RMLTC0005a-PostgreSQL",
                "RMLTC0006a-PostgreSQL",
                "RMLTC0007a-PostgreSQL",
                "RMLTC0007b-PostgreSQL",
                "RMLTC0007c-PostgreSQL",
                "RMLTC0007d-PostgreSQL",
                "RMLTC0007e-PostgreSQL",
                "RMLTC0007f-PostgreSQL",
                "RMLTC0007g-PostgreSQL",
                "RMLTC0008a-PostgreSQL",
                "RMLTC0008c-PostgreSQL",
                "RMLTC0010a-PostgreSQL",
                "RMLTC0010b-PostgreSQL",
                "RMLTC0010c-PostgreSQL",
                "RMLTC0011b-PostgreSQL",
                "RMLTC0012a-PostgreSQL",
                "RMLTC0012b-PostgreSQL",
                "RMLTC0019a-PostgreSQL",
                "RMLTC0019b-PostgreSQL",
                "RMLTC0020a-PostgreSQL",
                "RMLTC0020b-PostgreSQL"
        );

        return directories.stream().map(Arguments::of);
    }

    public static Stream<Arguments> negativeTests() {
        List<String> directories = List.of(
                "RMLTC0002e-PostgreSQL",
                "RMLTC0002g-PostgreSQL",
                "RMLTC0002h-PostgreSQL",
                "RMLTC0002i-PostgreSQL"
        );

        return directories.stream().map(Arguments::of);
    }

    @AfterAll
    public static void afterAll() {
        container.close();
    }

    @Override
    protected String getDbURL() {
        return container.getJdbcUrl();
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        // prepare the database
        String resourceFile = "src/test/resources/test-cases/postgres/" + directory + "/resource.sql";
        // some folders don't contain the file as the generation is supposed to be terminated before DB is queried
        if (Files.exists(Paths.get(resourceFile))) {
            prepareDatabase(resourceFile);
        }
        // read in the plan
        String plan = Files.readString(Paths.get("src/test/resources/test-cases/postgres/" + directory + "/mapping.ttl"));
        plan = plan.replace("CONNECTIONDSN", container.getJdbcUrl());
        this.positiveTestTurtlePlan("src/test/resources/test-cases/postgres/", directory, plan);
    }

    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        // prepare the database
        String resourceFile = "src/test/resources/test-cases/postgres/" + directory + "/resource.sql";
        // some folders don't contain the file as the generation is supposed to be terminated before DB is queried
        if (Files.exists(Paths.get(resourceFile))) {
            prepareDatabase(resourceFile);
        }
        // read in the plan
        String plan = Files.readString(Paths.get("src/test/resources/test-cases/postgres/" + directory + "/mapping.ttl"));
        plan = plan.replace("CONNECTIONDSN", container.getJdbcUrl());
        this.negativeTestTurtlePlan("src/test/resources/test-cases/postgres/", directory, plan);

    }
}
