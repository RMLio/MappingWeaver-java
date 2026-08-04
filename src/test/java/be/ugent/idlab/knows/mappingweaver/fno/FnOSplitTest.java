package be.ugent.idlab.knows.mappingweaver.fno;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * A function that produces several values, such as a split, inside a logical view.
 * <p>
 * The values all have to be mapped: splitting {@code "read write"} is meant to yield a
 * triple for {@code read} and one for {@code write}. Whether that happens depends on where
 * the function sits. A field of the logical view can produce a record per value, so the
 * field carries them all; an object map cannot, as the operator filling it in maps one
 * record to one record, so only the first value survives.
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

    @Disabled("A multi-valued function in an object map keeps only its first value: the "
            + "Extend operator maps one record to one record, so the other values are dropped")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("multiValuedObjectMap")
    public void multiValuedFunctionInAnObjectMap(String directory) throws Exception {
        this.positiveTest(BASE, directory);
    }

    @Disabled("Reading a field that the record does not have raises a MappingException, "
            + "which aborts the mapping instead of leaving the value unbound")
    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("multiValuedField")
    public void multiValuedFunctionInAField(String directory) throws Exception {
        this.positiveTest(BASE, directory);
    }
}
