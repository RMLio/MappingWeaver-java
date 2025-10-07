package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@Disabled
@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLCCTest extends TestCore {
    private static final List<String> testsFailed = List.of(
            //// positive, rust panic
            // Predicate Object Map BlankNode(BnodeId("riog00000003")) has 0 object maps
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/16
            "RMLTC-CC-0001-Alt",
            "RMLTC-CC-0001-Bag",
            "RMLTC-CC-0001-List",
            "RMLTC-CC-0001-Seq",
            "RMLTC-CC-0003-EB",
            "RMLTC-CC-0003-EL",
            "RMLTC-CC-0003-NEB",
            "RMLTC-CC-0003-NEL",
            "RMLTC-CC-0003-NELb",
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

            // Cannot parse term map type for BlankNode(BnodeId("riog00000002"))
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/17
            "RMLTC-CC-0004-SM1",
            "RMLTC-CC-0004-SM2",
            "RMLTC-CC-0004-SM3",
            "RMLTC-CC-0004-SM4",
            "RMLTC-CC-0004-SM5",

            // positive, Expected output path does not exist! --> Fix in TestCore (default.nq should also be allowed)
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-java/-/issues/17
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

    );

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(


        );
        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-cc/", directory);
    }
}

