package be.ugent.idlab.knows.mappingweaver.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;

import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

public class CliVerbosityTest {

    @Test
    void verbosityVVV_setsDebugLevel() throws Exception {
        ParseResult result = new CommandLine(CliCommand.create()).parseArgs("-m", "dummy.ttl", "-vvv");
        Main.applyVerbosity(result);
        assertEquals("debug", System.getProperty("org.slf4j.simpleLogger.log.be.ugent.idlab.knows.mappingweaver"));
    }
}
