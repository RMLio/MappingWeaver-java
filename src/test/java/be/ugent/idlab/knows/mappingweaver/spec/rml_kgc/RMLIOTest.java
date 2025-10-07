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

@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLIOTest extends TestCore {
    // all unfixable due to rust panic
    private static Stream<Arguments> unfixableTests() {
        return Stream.of(
                ///// positive, rust panic
                // Unsupported reference formulation: http://www.w3.org/ns/formats/SPARQL_Results_CSV
                // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/18
                "RMLSTC0003",

                // Unsupported reference formulation: http://w3id.org/rml/SQL2008Table && rust panic
                // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/18
                "RMLSTC0006a",

                // Complex reference formulation unsupported: ReferenceFormulation { iri: BlankNode(BnodeId("riog00000003")), kind: CustomReferenceFormulation { meta_data_graph: "_:riog00000003 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://w3id.org/rml/XPathReferenceFormulation>.\n_:riog00000003 <http://w3id.org/rml/namespace> _:riog00000004.\n_:riog00000004 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://w3id.org/rml/Namespace>.\n_:riog00000004 <http://w3id.org/rml/namespacePrefix> \"ex\".\n_:riog00000004 <http://w3id.org/rml/namespaceURL> \"http://example.org\".\n" } }
                // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/18
                "RMLSTC0007d"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> positive() {
        List<String> directories = List.of(
                "RMLSTC0001a",
                "RMLSTC0001b",
                "RMLSTC0002a",
                "RMLSTC0002b",
                "RMLSTC0002c",
                "RMLSTC0002d",
                "RMLSTC0002e",
                "RMLSTC0004a",
                "RMLSTC0004b",
                "RMLSTC0004c",
                "RMLSTC0006b",
                "RMLSTC0007a",
                "RMLSTC0007b",
                "RMLSTC0007c",
                "RMLSTC0008a",
                "RMLSTC0008b",
                "RMLSTC0009a",
                "RMLSTC0011a",
                "RMLSTC0011b",
                "RMLSTC0011c",
                "RMLSTC0011d",
                "RMLSTC0011e",
                "RMLSTC0012a",
                "RMLSTC0012e",

                "RMLTTC0000"
        );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negative() {
        return Stream.of().map(Arguments::of);
    }


    private static Stream<Arguments> positiveFailing() {
        return Stream.of(
                "RMLSTC0012b",
                "RMLSTC0012c",
                "RMLSTC0012d",

                "RMLTTC0001a",
                "RMLTTC0001b",
                "RMLTTC0001c",
                "RMLTTC0001d",
                "RMLTTC0001e",
                "RMLTTC0002a",
                "RMLTTC0002b",
                "RMLTTC0002c",
                "RMLTTC0002d",
                "RMLTTC0002e",
                "RMLTTC0002f",
                "RMLTTC0002g",
                "RMLTTC0002h",
                "RMLTTC0002i",
                "RMLTTC0002j",
                "RMLTTC0002k",
                "RMLTTC0002l",
                "RMLTTC0002m",
                "RMLTTC0002n",
                "RMLTTC0002o",
                "RMLTTC0002p",
                "RMLTTC0002q",
                "RMLTTC0002r",
                "RMLTTC0003a",
                "RMLTTC0004a",
                "RMLTTC0004b",
                "RMLTTC0004c",
                "RMLTTC0004d",
                "RMLTTC0004e",
                "RMLTTC0004f",
                "RMLTTC0004g",
                "RMLTTC0005a",
                "RMLTTC0005b",
                "RMLTTC0006a",
                "RMLTTC0006b",
                "RMLTTC0006c",
                "RMLTTC0006d",
                "RMLTTC0006e",
                "RMLTTC0007a"
        ).map(Arguments::of);
    }

    private static Stream<Arguments> negativeFailing() {
        return Stream.of("RMLSTC0010a",
                "RMLSTC0010b"
        ).map(Arguments::of);
    }

    private static boolean hasNegativeTests() {
        System.out.println("Checking for negative tests");
        return !negative().toList().isEmpty();
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positive")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-io/", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-io/", directory);
    }

    @Disabled("No passing negative testcases to run")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negative")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-io/", directory);
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeFailing")
    public void negativeFailingTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-io/", directory);
    }
}
