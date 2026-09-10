package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.CollectionNode;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LiteralTypeFunction
        implements ExtendFunction {
    private final ExtendFunction innerFunction;
    private final ExtendFunction languageFunction;
    private final ExtendFunction datatypeFunction;

    public LiteralTypeFunction(ExtendFunction function) {
        this(function, null);
    }

    public LiteralTypeFunction(ExtendFunction function, ExtendFunction languageFunction) {
        this(function, languageFunction, null);
    }

    public LiteralTypeFunction(ExtendFunction innerFunction, ExtendFunction languageFunction,
                               ExtendFunction datatypeFunction) {
        this.innerFunction = innerFunction;
        this.languageFunction = languageFunction;
        this.datatypeFunction = datatypeFunction;
    }

    /**
     * Turns a value the inner function produced into a literal, with the language and
     * datatype this function was given. A value standing for several terms gives a literal
     * per term, held together as one value the way the inner function handed it over.
     */
    @Nullable
    private RDFNode asLiteral(@Nullable RDFNode innerNode, @Nullable SolutionMapping mapping) {
        //extract inner value
        if (innerNode == null || innerNode.isNull()) {
            return null;
        }

        if (innerNode.isCollection()) {
            List<RDFNode> literals = new ArrayList<>();
            for (RDFNode member : ((CollectionNode) innerNode).members()) {
                RDFNode literal = asLiteral(member, mapping);
                if (literal != null) {
                    literals.add(literal);
                }
            }

            return literals.isEmpty() ? null : new CollectionNode(literals);
        }

        String language = (this.languageFunction == null) ? "" : this.languageFunction.apply(mapping).getFirst().getValue().toString();
        language = (language == null) ? "" : language;

        if (!language.isBlank() && !JSONPlanParser.allowedLanguagesPattern.matcher(language).find()) {
            throw new MappingException("Invalid language annotation: " + language);
        }

        String datatype = "http://www.w3.org/2001/XMLSchema#string";
        if (this.datatypeFunction == null) {
            // try to extract datatype
            if (innerNode instanceof LiteralNode) {
                datatype = ((LiteralNode) innerNode).getDatatype();
            }
            return new LiteralNode(innerNode.getValue().toString(), datatype, language);

        } else {
            List<RDFNode> typeURLResult = this.datatypeFunction.apply(mapping);
            String typeURL = typeURLResult.getFirst().getValue().toString();
            return new LiteralNode(innerNode.getValue(), typeURL, language);
        }
    }

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Literal);
    }

    @Override
    @Nullable
    public List<RDFNode> apply(@Nullable SolutionMapping solutionMapping) {
        List<RDFNode> literals = new ArrayList<>();
        List<RDFNode> innerResults = innerFunction.apply(solutionMapping);
        if (innerResults == null) {
            return null;
        }
        for (RDFNode innerNode : innerResults) {
            RDFNode literal = asLiteral(innerNode, solutionMapping);
            if (literal != null) {
                literals.add(literal);
            }
        }
        return literals;
    }

}
