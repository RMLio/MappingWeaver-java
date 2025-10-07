package be.ugent.idlab.knows.mappingweaver.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFType;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.JSONPlanParser;
import org.jspecify.annotations.Nullable;

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

    @Override
    @Nullable
    public RDFNode applyToNode(@Nullable SolutionMapping mapping) {

        RDFNode innerNode = this.innerFunction.applyToNode(mapping);

        //extract inner value
        if (innerNode == null || innerNode.isNull()) {
            return null;
        }

        String language = (this.languageFunction == null) ? "" : this.languageFunction.apply(mapping);
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
            return new LiteralNode(innerNode.getValue(), datatype, language);

        } else {
            String typeURL = this.datatypeFunction.apply(mapping);
            return new LiteralNode(innerNode.getValue(), typeURL, language);
        }
    }

    @Override
    public Optional<RDFType> getRDFTypeOpt() {
        return Optional.of(RDFType.Literal);
    }

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        return this.innerFunction.apply(solutionMapping);
    }

}
