package be.ugent.idlab.knows.mappingplan.parsing;

import be.ugent.idlab.knows.amo.blocks.Pair;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.ExtendOperator;
import be.ugent.idlab.knows.functions.agent.functionModelProvider.fno.exception.FnOException;
import be.ugent.idlab.knows.mappingplan.extend_functions.*;
import be.ugent.idlab.knows.mappingplan.extend_functions.fno.FnOFunction;
import be.ugent.idlab.knows.mappingplan.extend_functions.fno.FnOParameter;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Parser for construction of the Extend operator
 * Due to the complexity of the operator, a separate class was created
 */
public class ExtendOperatorParser {

    /**
     * A map for easy access to the different node types without an if-statement
     */
    private final Map<String, RDFType> nodeTypes = Map.of(
            "Iri", RDFType.IRI,
            "Literal", RDFType.Literal,
            "BlankNode", RDFType.Blank);

    /**
     * Main method for constructing the operator
     *
     * @param id       id of the operator (for debugging purposes)
     * @param config   a description of the operator
     * @param inputFragments fragments the operator reads from
     * @param outputFragments fragments the operator writes to
     * @return the constructed Extend operator
     */
    public Operator parse(String id, JSONObject config, Set<String> inputFragments, Set<String> outputFragments) {

        List<Pair<String, ExtendFunction>> extendDescriptions = new ArrayList<>();

        // keys are the new variables
        for (String key : config.keySet()) {
            JSONObject varDesc = config.getJSONObject(key);
            ExtendFunction extendFunction = parseExtendFunction(varDesc);
            extendDescriptions.add(new Pair<>(key, extendFunction));
        }

        return new ExtendOperator(id, inputFragments, outputFragments, extendDescriptions);
    }

    /**
     * A separate function for parsing the ExtendFunction: code for the actual
     * execution of the operator
     *
     * @param functionDescription a description of the function as JSON
     * @return an instance of ExtendFunction, reflecting the description
     */
    private ExtendFunction parseExtendFunction(JSONObject functionDescription) {
        if (functionDescription == null) {
            return null;
        }
        String funcType = functionDescription.getString("type");
        JSONObject innerFunc = functionDescription.optJSONObject("inner_function");

        ExtendFunction function = switch (funcType) {
            case "Iri" -> {
                String iri = functionDescription.isNull("base_iri") ? null : functionDescription.getString("base_iri");
                yield new IriTypeFunction(iri, parseExtendFunction(innerFunc));
            }
            case "BlankNode" -> new BlankTypeFunction(parseExtendFunction(innerFunc));
            case "Literal" -> {
                ExtendFunction datatypeFunction = parseExtendFunction(
                        functionDescription.optJSONObject("dtype_function"));
                ExtendFunction languageFunction = parseExtendFunction(
                        functionDescription.optJSONObject("langtype_function"));

                yield new LiteralTypeFunction(parseExtendFunction(innerFunc), languageFunction, datatypeFunction);

            }
            case "Reference" -> new ReferenceFunction(functionDescription.getString("value"));
            case "UriEncode" -> new EncodeUriFunction(parseExtendFunction(innerFunc));
            case "TemplateFunctionValue" -> {
                String template = functionDescription.getString("template");
                JSONArray array = functionDescription.getJSONArray("variable_function_pairs");
                Map<String, ExtendFunction> functionPairs = new HashMap<>();
                for (int i = 0; i < array.length(); i++) {
                    JSONArray pair = array.getJSONArray(i);
                    functionPairs.put(pair.getString(0), parseExtendFunction(pair.getJSONObject(1)));
                }
                yield new TemplateValueFunction(template, functionPairs);
            }
            case "Constant" -> new ConstantValueFunction(functionDescription.getString("value"));
            case "FnO" -> parseFnOFunction(functionDescription);
            case "Concatenate" -> {
                ExtendFunction left = parseExtendFunction(functionDescription.getJSONObject("left_value"));
                ExtendFunction right = parseExtendFunction(functionDescription.getJSONObject("right_value"));
                String separator = functionDescription.getString("separator");

                yield new ConcatenateFunction(left, right, separator);
            }
            case "Nop" -> new NopFunction();
            default -> (ExtendFunction) solutionMapping -> {
                throw new RuntimeException(
                        String.format("Extend function for type '%s' not implemented!", funcType));
            };
        };

        return function;
    }

    private ExtendFunction parseFnOFunction(JSONObject functionDescription) {
        String identifier = functionDescription.getString("fno_identifier");
        JSONObject parameters = functionDescription.getJSONObject("parameters");

        List<FnOParameter> fnOParameters = new ArrayList<>();

        for (String key : parameters.keySet()) {
            JSONObject jsonParameter = parameters.getJSONObject(key);
            String type = jsonParameter.getString("type");

            JSONObject extend = jsonParameter.has("inner_function") ? jsonParameter.getJSONObject("inner_function") : jsonParameter;

            ExtendFunction function = this.parseExtendFunction(extend);

            FnOParameter parameter = new FnOParameter(key, type, function);
            fnOParameters.add(parameter);
        }

        try {
            return new FnOFunction(identifier, fnOParameters);
        } catch (FnOException e) {
            throw new RuntimeException(e);
        }
    }
}
