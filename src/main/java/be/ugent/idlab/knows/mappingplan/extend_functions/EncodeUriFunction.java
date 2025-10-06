package be.ugent.idlab.knows.mappingplan.extend_functions;

import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.functions.ExtendFunction;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

import org.apache.commons.validator.routines.UrlValidator;
import org.jspecify.annotations.Nullable;

/**
 * ExtendFunction that returns an IRI node containing the URL, as specified by
 * the inner function
 *
 * @param uriEncodeInnerFuncJson a String with the JSON description of the inner
 *                               function
 */
public record EncodeUriFunction(ExtendFunction uriEncodeInnerFuncJson) implements ExtendFunction {

    @Override
    @Nullable
    public String apply(@Nullable SolutionMapping solutionMapping) {
        String innerValue = this.uriEncodeInnerFuncJson.apply(solutionMapping);
        //FIXME: Throwing an error instead, at the root(reference function) would be better.
        //       But this doesn't conform to the current test-cases.
        if(innerValue == null){
            return null;
        }
        //            if (!UrlValidator.getInstance().isValid(innerValue)) {
        return URLEncoder.encode(innerValue, StandardCharsets.UTF_8);
//            }
//            return innerValue;
    }
}
