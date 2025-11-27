package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import be.ugent.idlab.knows.mappingweaver.utilities.FlinkMiniClusterExtension;

@ExtendWith(FlinkMiniClusterExtension.class)
public class RMLFNMLTest extends TestCore {

    private static final List<String> testsFailed = List.of(
            //// positive, rust panic.
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19

            //// negative, rust panic.
            // No Triples were matched (cause not yet found).
            // waiting for https://gitlab.ilabt.imec.be/rml/proc/algemaploom-rs/-/issues/19

                
               
                


    );

    private static Stream<Arguments> unfixable() {
        return Stream.of(
            //"RMLFNMLTC0011-CSV" HTTP://VENUS IRI 
            "RMLFNMLTC0081-CSV" // NO CLUE
        ).map(Arguments::of);
    }

    private static Stream<Arguments> correct_ignored() {
        return Stream.of(
                "RMLFNMLTC0001-CSV", // CONTAINS RANDOM, BUT THIS IS CORRECT CURRENTLY
                "RMLFNMLTC0031-CSV",  // HTTP lowercase
                "RMLFNMLTC0061-CSV", // ACTUALLY CORRECT
                "RMLFNMLTC0003-CSV" // JUST WORKS

                ).map(Arguments::of);
    }

    private static Stream<Arguments> positiveTests() {
        List<String> directories = List.of(
                //"RMLFNMLTC0004-CSV" // length operator not working, also not working with string_length 's' is null
                 //"RMLFNMLTC0005-CSV" // cannot do uppercase because s is null
                 // "RMLFNMLTC0007-CSV" // Had to change test because param values were not in sync with the grel java repo, finally, an incorrect http://example.com/base was used for the literal
                 "RMLFNMLTC0008-CSV" // p_int_i_from 'from' is null
                // "RMLFNMLTC0021-CSV", // modeParam -> html  'mode' is null
                // "RMLFNMLTC0041-CSV", // toUppercase s is null ( string )
                // "RMLFNMLTC0051-CSV", // param find / replace, 'find is null'
                // "RMLFNMLTC0101-CSV",
                // "RMLFNMLTC0102-CSV",
                // "RMLFNMLTC0103-CSV",
                // "RMLFNMLTC0104-CSV"
               

        );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> negativeTests() {
        return Stream.of().map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }

    @Disabled
    @ParameterizedTest(name = "Negative test index: {index} Filename: {0}")
    @MethodSource("negativeTests")
    public void negativeTest(String directory) throws Exception {
        this.negativeTest("src/test/resources/spec/rml_kgc/rml-fnml/", directory);
    }
}

