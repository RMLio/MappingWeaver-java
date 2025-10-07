/*
    TestRunner executes every test-class in rml_kgc. It keeps information of executed/passed tests in
    a summary. Output is stored in a static list to show at the end of the program.

    !! Only RMLCoreTest implemented for now. When other classes are implemented, they need to be added to
    selectors of the global summary. Also for each new class, there needs to be a testClass(...). !!

    Dependency platform.launcher is added to make an execution like this possible. To get a global summary and
    different summaries for each class, different launchers/listeners are used. This is a bit tedious but
    will suffice for this simple feature.

    Author: Stijn Van Biesen.
*/

package be.ugent.idlab.knows.mappingweaver.spec.rml_kgc;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

public class TestRunner {
    private static final List<String> output = new ArrayList<>();
    public static void main(String[] args) {
        // Total coverage.
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder
                .request()
                .selectors(
                        selectClass(RMLCoreTest.class),
                        selectClass(RMLCCTest.class),
                        selectClass(RMLFNMLTest.class),
                        selectClass(RMLIOTest.class),
                        selectClass(RMLLVTest.class),
                        selectClass(RMLSTARTest.class)
                )
                .build();

        Launcher launcher = LauncherFactory.create();

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);

        launcher.execute(request);

        TestExecutionSummary summary = listener.getSummary();
        long testFound = summary.getTestsFoundCount();
        long testPassed = summary.getTestsSucceededCount();

        output.add(String.format("Coverage for rml_kgc = %d%%",100*testPassed/testFound));

        // Coverages per class.
        testClass(RMLCoreTest.class);
        testClass(RMLCCTest.class);
        testClass(RMLFNMLTest.class);
        testClass(RMLIOTest.class);
        testClass(RMLLVTest.class);
        testClass(RMLSTARTest.class);


        // Printing coverage.
        for (String line: output) {
            System.out.println(line);
        }
    }

    public static void testClass(Class<?> c) {
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder
                .request()
                .selectors(selectClass(c))
                .build();

        Launcher launcher = LauncherFactory.create();

        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);

        launcher.execute(request);

        TestExecutionSummary summary = listener.getSummary();
        long testFound = summary.getTestsFoundCount();
        long testPassed = summary.getTestsSucceededCount();

        String moduleName = "<unknown module>";
        try {
            Method getModuleMethod = c.getMethod("getModule");
            moduleName = (String) getModuleMethod.invoke(null);
        } catch (Exception ignored) {
        }
        output.add(String.format("\tCoverage for %s = %d%%", moduleName,100*testPassed/testFound));
    }
}
