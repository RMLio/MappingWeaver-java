package be.ugent.idlab.knows.mappingweaver.fno;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * A function that produces several values, such as a split, inside a logical view.
 * <p>
 * The values all have to be mapped: splitting {@code "read write"} yields a triple for
 * {@code read} and one for {@code write}. This holds wherever the function sits, in a
 * field of the logical view or in an object map, and whether the split is the outermost
 * function or fills in a parameter of another one.
 * <p>
 * These cases are written in RML-FNML's current vocabulary ({@code rml:functionExecution},
 * {@code rml:input}). The mappings they came from used the older
 * {@code fnml:functionValue} with {@code rr:predicateObjectMap}, which MappingLoom cannot
 * translate at all: it panics before a plan is produced.
 */
public class FnOSplitTest extends TestCore {

    private static final String BASE = "src/test/resources/test-cases/fno/";

    /**
     * A split in an object map, which should map every value it produces.
     */
    private static Stream<Arguments> multiValuedObjectMap() {
        return Stream.of(
                // splitting the field directly
                "RMLFNOTC1001-JSON",
                // nulls turned into an empty string before splitting
                "RMLFNOTC1002-JSON",
                // empty strings filtered out after splitting, with idlab-fn:trueCondition
                "RMLFNOTC1003-JSON"
        ).map(Arguments::of);
    }

    /**
     * The same split, as a field of the logical view.
     */
    private static Stream<Arguments> multiValuedField() {
        return Stream.of("RMLFNOTC1004-JSON").map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("multiValuedObjectMap")
    public void multiValuedFunctionInAnObjectMap(String directory) throws Exception {
        this.positiveTest(BASE, directory);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("multiValuedField")
    public void multiValuedFunctionInAField(String directory) throws Exception {
        this.positiveTest(BASE, directory);
    }
}
