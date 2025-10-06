package be.ugent.idlab.knows.spec.rmlio;

import be.ugent.idlab.knows.cores.TestCore;
import be.ugent.idlab.knows.utilities.FlinkMiniClusterExtension;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

@ExtendWith(FlinkMiniClusterExtension.class)
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class RMLIOFNOTest extends TestCore {
    private static final List<String> testsFailed = List.of(
            // Function with id http://example.com/idlab/function/random not found

            // Function with id http://example.com/idlab/function/toUpperCaseURL not found

            // Specified reference attribute 'WrongReference' not present in the input data

            //  Expected BNode or IRI: Got: [COMMA]

            // Actual differs from expected.


            // Problem in Serializer.

            // Function with id http://example.com/idlab/function/generateNull not found

            // Function with id http://users.ugent.be/~bjdmeest/function/grel.ttl#{Class} not found

            // JSONObject["join_alias"] not found.

            // Specified reference attribute 'authors[*]' not present in the input data

            // JSONObject["base_iri"] is not a string (class org.json.JSONObject$Null : null)

            // No suitable method 'get' with matching parameter types found in class 'io.fno.grel.ArrayFunctions'

            // Function with id http://users.ugent.be/~bjdmeest/function/grel.ttl#text not found

            // Function with id http://example.com/idlab/function/trueCondition not found

            // Function with id http://example.com/idlab/function/readFile not found


    );

    private static Stream<Arguments> positive() {
        List<String> directories = List.of(
                "RMLFNOTC0001-CSV",
                "RMLFNOTC0002-CSV",
                "RMLFNOTC0003-CSV",
                "RMLFNOTC0008-CSV",
                "RMLFNOTC0013-CSV",
                "RMLFNOTC0018-CSV",

                "RMLFNOTCAB0003-CSV",
                "RMLFNOTCAB0004-CSV",
                "RMLFNOTCAB0005-CSV",
                "RMLFNOTCAB0006-CSV"
                );
        return directories.stream().map(Arguments::of);
    }

    private static Stream<Arguments> positiveFailing() {
        List<String> directories = List.of(
                "RMLFNOTC0000-CSV",
                "RMLFNOTC0004-CSV",
                "RMLFNOTC0005-CSV",
                "RMLFNOTC0006-CSV",
                "RMLFNOTC0007-CSV",


                "RMLFNOTC0009-CSV",
                "RMLFNOTC0010-CSV",
                "RMLFNOTC0011-CSV",
                "RMLFNOTC0012-CSV",


                "RMLFNOTC0014-CSV",
                "RMLFNOTC0015-CSV",
                "RMLFNOTC0016-CSV",
                "RMLFNOTC0017-CSV",

                "RMLFNOTC0019-CSV",
                "RMLFNOTC0020-JSON",
                "RMLFNOTC0021-JSON",

                "RMLFNOTC0022-CSV",
                "RMLFNOTC0023-CSV",
                "RMLFNOTC0024-JSON",

                "RMLFNOTCA001",
                "RMLFNOTCA004",
                "RMLFNOTCA004b",

                "RMLFNOTCA005",
                "RMLFNOTCA006",

                "RMLFNOTCAB0007-JSON");
        return directories.stream().map(Arguments::of);
    }

    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positive")
    public void positiveTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/fno/", directory + '/');
    }

    @Disabled("Not running known failing test cases in CI")
    @ParameterizedTest(name = "Positive test index: {index} Filename: {0}")
    @MethodSource("positiveFailing")
    public void positiveFailingTest(String directory) throws Exception {
        super.positiveTest("src/test/resources/spec/rmlio/fno/", directory + '/');
    }
}
