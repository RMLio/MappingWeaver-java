package be.ugent.idlab.knows.mappingweaver.fixedbugs;

import be.ugent.idlab.knows.mappingweaver.cores.TestCore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

/**
 * A logical view read by more than one triples map.
 * <p>
 * The plan holds the view's fields once per triples map reading it, so a view used by six
 * triples maps arrives with every field six times over. The source operator combines its
 * fields with one another, so combining a field with itself multiplies the records it
 * produces by itself: a field iterating over 13 items, repeated six times, yields 13^6
 * records and exhausts the heap.
 */
public class LogicalViewFieldsTest extends TestCore {

    private static final String BASE = "src/test/resources/test-cases/rml_kgc/lv/";

    private static Stream<Arguments> positiveTests() {
        return Stream.of(
                // three triples maps reading one view whose field iterates over three
                // items: nine triples, not 3^3 x 3
                "RMLLVTC2002-JSON",
                // the same shape at the size it was reported at: six triples maps, a view
                // with nested iterators over a questionnaire
                "RMLLVTC2001-JSON"
        ).map(Arguments::of);
    }

    @ParameterizedTest(name = "Index: {index} Filename: {0}")
    @MethodSource("positiveTests")
    public void positiveTest(String directory) throws Exception {
        this.positiveTest(BASE, directory);
    }
}
