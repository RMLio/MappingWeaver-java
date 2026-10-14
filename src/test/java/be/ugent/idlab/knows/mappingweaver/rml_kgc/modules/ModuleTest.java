package be.ugent.idlab.knows.mappingweaver.rml_kgc.modules;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * The everyday check of the RML-KGC modules: each folder {@code modules/<module>/<group>} merges kg-construct test
 * cases that MappingWeaver passes into one mapping, whose expected output is the union of theirs (HANDBOOK:
 * Test resource organization).
 */
@Execution(ExecutionMode.CONCURRENT)
public class ModuleTest extends TestCore {
    private static final Path MODULES = Path.of("src/test/resources/rml_kgc/test-cases/modules");

    private static Stream<String> groups() throws IOException {
        try (Stream<Path> groups = Files.walk(MODULES, 2)) {
            return groups.filter(group -> Files.isRegularFile(group.resolve("mapping.ttl")))
                    .map(group -> group.getParent().getFileName() + "/" + group.getFileName())
                    .sorted()
                    .toList()
                    .stream();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("groups")
    public void group(String group) throws Exception {
        isomorphicTest(MODULES.resolve(group));
    }
}
