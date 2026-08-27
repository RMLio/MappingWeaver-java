package be.ugent.idlab.knows.mappingweaver.fno;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * A function that produces several values, such as a split, inside a logical view.
 * <p>
 * Every value has to be mapped: splitting {@code "read write"} yields a triple for
 * {@code read} and one for {@code write}, wherever the function is used.
 */
public class FnOSplitTest extends TestCore {

    private static final String BASE = "src/test/resources/test-cases/rml_kgc/fnml/";

    /**
     * A split in an object map, which should map every value it produces.
     */
    private static Stream<Arguments> positivePassing() {
        return Stream.of(
                // splitting the field directly
                "RMLFNOTC1001-JSON",
                // nulls turned into an empty string before splitting
                "RMLFNOTC1002-JSON",
                // empty strings filtered out after splitting, with idlab-fn:trueCondition
                "RMLFNOTC1003-JSON",
                // the same split as a logical-view field
                "RMLFNOTC1004-JSON",
                // an invalid return resource falls back to the function's first return
                "RMLFNOTC1005-JSON"
                // false negative: FnO cannot describe the datatype of array members
                // "RMLFNOTC1006-JSON"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positivePassing")
    public void positivePassingTest(String directory) throws Exception {
        this.positiveTest(BASE, directory, true);
    }

}
