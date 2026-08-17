package be.ugent.idlab.knows.mappingweaver.rml_kgc.spec;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class RMLCCTest extends TestCore {

    public static String getModule() {
        return "rml_kgc/rml-cc";
    }

    // All RML-CC test cases are positive (README: "**Error expected?** No"), but none
    // currently pass: RML Collections & Containers (rml:gather / rml:gatherAs) is not
    // implemented. 24 cases crash while translating the gather, the other 11 run but
    // silently drop the gathered collection/container (rdf:Alt/Bag/Seq and rdf:List),
    // so their output does not match the expected result.
    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                // Crash: translation of rml:gather panics, no output is produced
                "RMLTC-CC-0001-Alt",
                "RMLTC-CC-0001-Bag",
                "RMLTC-CC-0001-List",
                "RMLTC-CC-0001-Seq",
                "RMLTC-CC-0003-EB",
                "RMLTC-CC-0003-EL",
                "RMLTC-CC-0003-NEB",
                "RMLTC-CC-0003-NEL",
                "RMLTC-CC-0003-NELb",
                "RMLTC-CC-0004-SM1",
                "RMLTC-CC-0004-SM2",
                "RMLTC-CC-0004-SM3",
                "RMLTC-CC-0004-SM4",
                "RMLTC-CC-0004-SM5",
                "RMLTC-CC-0005-App1",
                "RMLTC-CC-0005-App2",
                "RMLTC-CC-0005-Car1",
                "RMLTC-CC-0005-Car2",
                "RMLTC-CC-0006-IT0",
                "RMLTC-CC-0006-IT3",
                "RMLTC-CC-0007-NES",
                "RMLTC-CC-0009-DUP-Bag",
                "RMLTC-CC-0009-DUP-List",
                "RMLTC-CC-0010-Lista",

                // Wrong output: runs, but the gathered collection/container isthrown away, so only the link triples are produced
                "RMLTC-CC-0002-Bag",
                "RMLTC-CC-0002-List",
                "RMLTC-CC-0003-EL-BN",
                "RMLTC-CC-0003-EL-Named",
                "RMLTC-CC-0006-IT1",
                "RMLTC-CC-0006-IT2",
                "RMLTC-CC-0006-IT4",
                "RMLTC-CC-0006-IT5",
                "RMLTC-CC-0008-ROMa",
                "RMLTC-CC-0008-ROMb",
                "RMLTC-CC-0010-Listb"
        ).map(Arguments::of);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/rml_kgc/spec/rml-cc/", directory + '/');
    }
}
