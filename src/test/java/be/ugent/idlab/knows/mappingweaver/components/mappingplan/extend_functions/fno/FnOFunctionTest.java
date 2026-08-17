package be.ugent.idlab.knows.mappingweaver.components.mappingplan.extend_functions.fno;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;
import be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions.fno.FnOFunction;

/**
 * Tests for FnOFunction description resolution: classpath:// routing, filesystem fallback,
 * and custom-description priority over built-ins.
 */
class FnOFunctionDescriptionTest {

    @TempDir
    Path tempDir;

    @AfterEach
    void resetToDefaults() {
        FnOFunction.configure(List.of(), false);
    }

    /** Triggers model loading without executing any function. */
    private void triggerLoad() throws FnOException {
        var fn = new FnOFunction("https://example.org/fn", List.of(), null);
        assertNotNull(fn);
    }

    private Path writeTtl(String filename, String content) throws IOException {
        Path file = tempDir.resolve(filename);
        Files.writeString(file, content);
        return file;
    }

    @Test
    void defaultDescriptionsLoadFromClasspath() {
        // built-in classpath:// descriptions must resolve without configure() being called
        assertDoesNotThrow(this::triggerLoad);
    }

    @Test
    void classpathPrefixedMissingResourceThrows() {
        FnOFunction.configure(List.of("classpath://no-such-functions.ttl"), true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, this::triggerLoad);
        assertTrue(ex.getMessage().contains("no-such-functions.ttl"),
                "error message should identify the missing resource");
    }

    @Test
    void filesystemPathNotFoundThrows() {
        String absent = tempDir.resolve("absent.ttl").toString();
        FnOFunction.configure(List.of(absent), true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, this::triggerLoad);
        assertTrue(ex.getMessage().contains("absent.ttl"),
                "error message should identify the missing file");
    }

    @Test
    void validFilesystemDescriptionIsLoaded() throws IOException, FnOException {
        Path ttl = writeTtl("extra.ttl", "@prefix ex: <https://example.org/> .\n");
        FnOFunction.configure(List.of(ttl.toString()), true);
        assertDoesNotThrow(this::triggerLoad);
    }

    @Test
    void customFilesystemDescriptionCombinedWithDefaults() throws IOException, FnOException {
        Path ttl = writeTtl("extra.ttl", "@prefix ex: <https://example.org/> .\n");
        FnOFunction.configure(List.of(ttl.toString()), false);
        assertDoesNotThrow(this::triggerLoad);
    }

    @Test
    void invalidTurtleThrowsWithParseError() throws IOException {
        Path ttl = writeTtl("broken.ttl", "this is not valid turtle !!!\n");
        FnOFunction.configure(List.of(ttl.toString()), true);
        IllegalStateException ex = assertThrows(IllegalStateException.class, this::triggerLoad);
        assertTrue(ex.getMessage().contains("broken.ttl"),
                "error message should identify the file that failed to parse");
    }

    @Test
    void customAbsolutePathWithSameFilenameAsDefaultIsLoadedFromFilesystem() throws IOException, FnOException {
        // Absolute-path custom entry with the same filename as a built-in is loaded from disk
        // (openResource checks the filesystem first) and combined with the remaining defaults.
        Path ttl = writeTtl("functions_grel.ttl", "@prefix ex: <https://example.org/> .\n");
        FnOFunction.configure(List.of(ttl.toAbsolutePath().toString()), false);
        assertDoesNotThrow(this::triggerLoad);
    }
}
