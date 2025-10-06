package be.ugent.idlab.knows;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.ExtendOperator;
import be.ugent.idlab.knows.mappingplan.parsing.ExtendOperatorParser;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExtendParserTest {
    ExtendOperatorParser parser = new ExtendOperatorParser();

    @SuppressWarnings("null")
    @Test
    public void parseBlankNodeTemplateString() {
        String input = """
                {
                    "?tm0_sm": {
                      "type": "BlankNode",
                      "inner_function": {
                        "type": "TemplateFunctionValue",
                        "template": "students{ID}",
                        "variable_function_pairs": [

                            [
                                "ID",
                                {
                                    "type": "Constant",
                                    "value": "Foo"
                                }
                            ]
                        ]
                      }
                    }
                  }
                """;
        ExtendOperator op = (ExtendOperator) this.parser.parse("ExtendOp", new JSONObject(input), Set.of("f_default"), Set.of("f_default"));
        MappingTuple in = new MappingTuple();
        in.addSolutionMap("f_default", new SolutionMapping(Map.of("ID", new LiteralNode("Foo"))));
        MappingTuple out = op.apply(in);
        out.getSolutionMappings("f_default");

        RDFNode value = out.getSolutionMappings("f_default").stream().toList().get(0).get("?tm0_sm");

        assertTrue(value.isBlank());
        assertEquals(value.getValue().toString(), "studentsFoo");
    }

    @SuppressWarnings("null")
    @Test
    public void templateStringReplaceMultiple() {
        String config = """
                {
                    "?tm0_o0_0": {
                        "type": "Literal",
                        "inner_function": {
                            "type": "TemplateFunctionValue",
                            "template": "{FirstName} {LastName}",
                            "variable_function_pairs": [

                                [
                                    "FirstName",
                                    {
                                        "type": "Constant",
                                        "value": "Venus"
                                    }
                                ],
                                [
                                    "LastName",
                                    {
                                        "type": "Constant",
                                        "value": "Williams"
                                    }
                                ]
                            ]
                        },
                        "dtype_function": null,
                        "langtype_function": null
                    }
                }
                """;

        SolutionMapping input = new SolutionMapping(
                Map.of(
                        "FirstName", new LiteralNode("Venus"),
                        "LastName", new LiteralNode("Williams")));
        MappingTuple inputTuple = new MappingTuple();
        inputTuple.addSolutionMap("f_default", input);

        ExtendOperator op = (ExtendOperator) parser.parse("ExtendOp", new JSONObject(config), Set.of("f_default"), Set.of("f_default"));
        MappingTuple out = op.apply(inputTuple);

        RDFNode value = out.getSolutionMappings("f_default").stream().toList().get(0).get("?tm0_o0_0");
        assertTrue(value.isLiteral());
        assertEquals(value.getValue().toString(), "Venus Williams");
    }
}
