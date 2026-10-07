package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import org.jspecify.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

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
     * @return a List of RDFNode with the filled in template
     */
    @Nullable
    private List<RDFNode> executeTemplateFunctions(final String template, Map<String, List<RDFNode>> variableValuePairs) {
        // scan for places in the string where variables are
        // variables can be located by them being enclosed by braces
        String templateCopy = template;
        int index = templateCopy.indexOf('{');
        boolean nullDetected = false;
        while (index != -1) {
            // check for escaping
            if (index > 0 && templateCopy.charAt(index - 1) == '\\') {
                index = templateCopy.indexOf('{', index + 1);
                continue;
            }
            // find closing bracket
            int end = templateCopy.indexOf('}', index);
            String variableName = templateCopy.substring(index + 1, end);
            List<RDFNode> values = variableValuePairs.get(variableName);
            if (values == null) {
                nullDetected = true;
                break;
            }
            if (values.size() > 1) {
                throw new IllegalArgumentException("Template variable substitution error: variable " + variableName + " has multiple values: " + values);
            }
            String value = values.getFirst().getValue().toString();

            templateCopy = templateCopy.substring(0, index) + value + templateCopy.substring(end + 1);
            index = templateCopy.indexOf('{');
        }

        if (nullDetected) {
            return null;
        }

        templateCopy = templateCopy.replace("\\{", "{");
        templateCopy = templateCopy.replace("\\}", "}");
        return List.of(new LiteralNode(templateCopy));
    }

    //FIXME: Throwing an error instead, at the root(reference function) would be better.
    //       But this doesn't conform to the current test-cases.
    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        Map<String, List<RDFNode>> executedValues = this.varFunctionPairs()
                .entrySet()
                .stream()
                .map(entry -> {
                    List<RDFNode> result = entry.getValue().apply(solutionMapping);
                    return result != null ? Map.entry(entry.getKey(), result) : null;
                })
                .filter(Objects::nonNull)  // Filter out null entries
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return this.executeTemplateFunctions(template, executedValues);
    }


}
