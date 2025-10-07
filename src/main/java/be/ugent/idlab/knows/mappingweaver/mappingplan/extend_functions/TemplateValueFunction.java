package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;

/**
 * ExtendFunction that returns the result of applying the template to the
 * solution mapping
 *
 * @param template a String with template to be applied
 * @param varFunctionPairs     mapping of variable name -> function to execute
 */
public record TemplateValueFunction(String template, Map<String, ExtendFunction> varFunctionPairs)
        implements ExtendFunction {

    /**
     * Fills the templated string: replacing any variables between { } with the
     * value in the Solution Mapping
     *
     * @param template           a String with template to be filled
     * @param variableValuePairs a SolutionMapping to grab the values of the
     *                           variables from
     * @return a String with the filled in template
     */
    @Nullable
    private String executeTemplateFunctions(String template, Map<String, @Nullable String> variableValuePairs) {
        // scan for places in the string where variables are
        // variables can be located by them being enclosed by braces
        int index = template.indexOf('{');
        boolean nullDetected = false;
        while (index != -1) {
            // check for escaping
            if (index > 0 && template.charAt(index - 1) == '\\') {
                index = template.indexOf('{', index + 1);
                continue;
            }
            // find closing bracket
            int end = template.indexOf('}', index);
            String variableName = template.substring(index + 1, end);
            Object value = variableValuePairs.get(variableName);
            if (value == null) {
                nullDetected = true;
                break;
            }

            template = template.substring(0, index) + value.toString() + template.substring(end + 1);
            index = template.indexOf('{');
        }

        if (nullDetected) {
            return null;
        }

        template = template.replace("\\{", "{");
        template = template.replace("\\}", "}");
        return template;
    }

    //FIXME: Throwing an error instead, at the root(reference function) would be better.
    //       But this doesn't conform to the current test-cases.
    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        Map<String, String> executedValues = this.varFunctionPairs()
                .entrySet()
                .stream()
                .map(entry -> {
                    String result = entry.getValue().apply(solutionMapping);
                    return result != null ? Map.entry(entry.getKey(), result) : null;
                })
                .filter(Objects::nonNull)  // Filter out null entries
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return this.executeTemplateFunctions(template, executedValues);
    }


}
