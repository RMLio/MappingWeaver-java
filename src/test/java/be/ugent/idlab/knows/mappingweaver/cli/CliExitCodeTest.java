package be.ugent.idlab.knows.mappingweaver.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The CLI's exit codes, on which a test runner or benchmark judges a run: 0 for a successful run, 1 for invalid
 * arguments and for every failure.
 */
public class CliExitCodeTest {

    private static final Path CASES = Path.of("src/test/resources/rml_kgc/test-cases/engines/cli");

    @TempDir
    Path tmp;

    @Test
    void unknownOption_exits1() {
        assertEquals(1, Main.run("--unknown-option"));
    }

    @Test
    void missingMapping_exits1() {
        assertEquals(1, Main.run("toFile", "-o", tmp.resolve("out").toString()));
    }

    @Test
    void toFileWithoutOutputPath_exits1() {
        assertEquals(1, Main.run("-m", CASES.resolve("RMLTC0001a-JSON/mapping.ttl").toString(), "toFile"));
    }

    @Test
    void help_exits0() {
        assertEquals(0, Main.run("--help"));
    }

    @Test
    void version_printsTheVersionAndExits0() {
        PrintStream original = System.out;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
        try {
            assertEquals(0, Main.run("--version"));
        } finally {
            System.setOut(original);
        }
        assertEquals("MappingWeaver " + CliCommand.version(), out.toString(StandardCharsets.UTF_8).strip());
    }

    @Test
    void mappingError_exits1() throws IOException {
        Path mapping = copyCase("RMLTC0002e-JSON");
        assertEquals(1, Main.run("-m", mapping.toString(), "toFile", "-o", tmp.resolve("out").toString()));
    }

    @Test
    void failureDuringJob_exits1() throws IOException {
        Path mapping = copyCase("RMLTC0001a-JSON");
        Files.writeString(mapping.resolveSibling("student.json"), "{\"students\": [{\"ID\": 10, \"Name\":");
        assertEquals(1, Main.run("-m", mapping.toString(), "toFile", "-o", tmp.resolve("out").toString()));
    }

    @Test
    void boundedToFile_exits0AndLeavesOnlyCompleteFiles() throws IOException {
        Path mapping = copyCase("RMLTC0001a-JSON");
        Path out = tmp.resolve("out");

        assertEquals(0, Main.run("-m", mapping.toString(), "toFile", "-o", out.toString()));

        List<Path> files;
        try (Stream<Path> walk = Files.walk(out)) {
            files = walk.filter(Files::isRegularFile).toList();
        }
        assertTrue(files.stream().noneMatch(f -> f.getFileName().toString().contains("inprogress")),
                "in-progress part files left: " + files);
        Set<String> actual = files.stream().flatMap(CliExitCodeTest::lines).collect(Collectors.toSet());
        Set<String> expected = lines(mapping.resolveSibling("output.nq")).collect(Collectors.toSet());
        assertEquals(expected, actual);
    }

    private Path copyCase(String id) throws IOException {
        Path dir = Files.createDirectory(tmp.resolve(id));
        try (Stream<Path> files = Files.list(CASES.resolve(id))) {
            for (Path f : files.toList()) {
                Files.copy(f, dir.resolve(f.getFileName()));
            }
        }
        return dir.resolve("mapping.ttl");
    }

    private static Stream<String> lines(Path file) {
        try {
            return Files.readAllLines(file).stream().map(String::strip).filter(l -> !l.isEmpty());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
