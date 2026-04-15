package be.ugent.idlab.knows.mappingweaver.mappingplan.parsing;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.json.JSONArray;
import org.json.JSONObject;

import be.ugent.idlab.knows.amo.blocks.nodes.IRINode;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.blocks.nodes.RDFNode;
import be.ugent.idlab.knows.amo.functions.JoinCondition;
import be.ugent.idlab.knows.amo.operators.Operator;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.NaturalJoinOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.binary.ThetaJoinOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.FragmenterOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.ProjectOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.RenameOperator;
import be.ugent.idlab.knows.amo.operators.intermediate.unary.TemplateSerializer;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.CSVSourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.DataIOSourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.JSONSourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.XMLSourceOperator;
import be.ugent.idlab.knows.amo.operators.source.dataio.fields.Field;
import be.ugent.idlab.knows.amo.operators.source.dataio.fields.FieldBuilder;
import be.ugent.idlab.knows.amo.operators.source.dataio.fields.ReferenceFormulation;
import be.ugent.idlab.knows.amo.operators.target.TargetOperator;
import be.ugent.idlab.knows.dataio.access.Access;
import be.ugent.idlab.knows.dataio.access.DatabaseType;
import be.ugent.idlab.knows.dataio.access.LocalFileAccess;
import be.ugent.idlab.knows.dataio.access.RDBAccess;
import be.ugent.idlab.knows.dataio.access.WebSocketAccess;
import be.ugent.idlab.knows.dataio.compression.Compression;
import be.ugent.idlab.knows.mappingweaver.exceptions.MappingException;
import be.ugent.idlab.knows.mappingweaver.flink.operators.FlinkTargetOperator;
import be.ugent.idlab.knows.mappingweaver.flink.source.KafkaSourceOperator;
import be.ugent.idlab.knows.mappingweaver.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingweaver.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingweaver.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.mappingweaver.mappingplan.fragment_functions.CopyFragmentFunction;
import be.ugent.idlab.knows.mappingweaver.mappingplan.join_conditions.EqualityJoinCondition;
import be.ugent.idlab.knows.mappingweaver.mappingplan.parsing.Adjacency.Fragment;

/**
 * Class for parsing the JSON descriptions of the operators
 */
public class JSONPlanParser implements Serializable {

    public static final Pattern languagePattern = Pattern.compile("\\?.*?@(.*)");
    // pattern straight from Mapper
    public static final Pattern allowedLanguagesPattern = Pattern.compile("^((?:(en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)|(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang))|((?:([A-Za-z]{2,3}(-(?:[A-Za-z]{3}(-[A-Za-z]{3}){0,2}))?)|[A-Za-z]{4})(-(?:[A-Za-z]{4}))?(-(?:[A-Za-z]{2}|[0-9]{3}))?(-(?:[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*(-(?:[0-9A-WY-Za-wy-z](-[A-Za-z0-9]{2,8})+))*(-(?:x(-[A-Za-z0-9]{1,8})+))?)|(?:x(-[A-Za-z0-9]{1,8})+))$");

    private final String basePath;
    private final String defaultBaseIRI;


    /**
     * Private constructor, as this class is only meant to be used through static
     * methods
     *
     * @param basePath base path for resolving the local paths
     * @param defaultBaseIRI The default base IRI
     */
    private JSONPlanParser(String basePath, String defaultBaseIRI) {
        this.basePath = basePath;
        this.defaultBaseIRI = defaultBaseIRI;
    }

    /**
     * Create a MappingPlan based on the description of operators in the specified
     * file
     *
     * @param env  environment to build the operators under
     * @param path path to the description file
     * @param defaultBaseIRI The default base IRI
     * @return a MappingPlan
     * @throws IOException when an IO error occurs while reading the file
     */
    public static MappingPlan fromFile(StreamExecutionEnvironment env, String path, String defaultBaseIRI) throws IOException {
        File file = new File(path);
        try (FileInputStream fis = new FileInputStream(path)) {
            String json = new String(fis.readAllBytes());
            return new JSONPlanParser(file.getParent() + "/", defaultBaseIRI).parse(env, json);
        }
    }

    /**
     * Create a MappingPlan based on the description of operators specified as a
     * JSON string
     *
     * @param env      environment to create operators under
     * @param json     JSON string with the description of the operators
     * @param basePath base path for file resolution
     * @return a MappingPlan
     */
    public static MappingPlan fromString(StreamExecutionEnvironment env, String json, String basePath, String defaultBaseIRI) {
        return new JSONPlanParser(basePath, defaultBaseIRI).parse(env, json);
    }

    /**
     * Create a MappingPlan based on the description of operators specified as a
     * JSON string
     *
     * @param env  environment to create operators under
     * @param json JSON string with the description of the operators
     * @return a MappingPlan
     */
    private MappingPlan parse(StreamExecutionEnvironment env, String json) {
        JSONObject obj = new JSONObject(json);
        JSONArray nodes = obj.getJSONArray("nodes");
        JSONArray edges = obj.getJSONArray("edges");
        Adjacency adj = new Adjacency(edges, nodes.length());
        List<Operator> operators = parseOperators(nodes, adj);

        OperatorGraph graph = new OperatorGraph(operators, adj);

        return new MappingPlan(env, graph, new GraphOpVisitor(env, graph));
    }

    /**
     * Parses the operators from the JSONArray of nodes, given an Adjacency
     *
     * @param nodes nodes as found in the JSON description
     * @param adj   Adjacency describing the connections between the operators.
     *              Adjacency is used to configure fragments for the operators
     * @return a list of operators
     */
    private List<Operator> parseOperators(JSONArray nodes, Adjacency adj) {
        List<Operator> operators = new ArrayList<>();
        ExtendOperatorParser extendParser = new ExtendOperatorParser(defaultBaseIRI);

        for (int i = 0; i < nodes.length(); i++) {
            Set<Fragment> incomingFragments = adj.getIncomingFragments(i);
            Set<String> incomingFragmentNames = incomingFragments.stream().map(Fragment::name).collect(Collectors.toSet());
            Set<Fragment> outgoingFragments = adj.getOutgoingFragments(i);
            Set<String> outgoingFragmentNames = outgoingFragments.stream().map(Fragment::name).collect(Collectors.toSet());

            JSONObject node = nodes.getJSONObject(i);
            String id = node.getString("id");
            JSONObject operator = node.getJSONObject("operator");
            JSONObject operatorConfig = operator.getJSONObject("config");

            Operator op = switch (operator.getString("type")) {
                case "SourceOp" -> parseSourceOperator(id, operatorConfig, outgoingFragmentNames);
                case "ProjectOp" -> parseProjectOperator(id, operatorConfig, incomingFragmentNames, outgoingFragmentNames);
                case "ExtendOp" -> extendParser.parse(id, operatorConfig, incomingFragmentNames, outgoingFragmentNames);
                case "SerializerOp" -> parseSerializerOperator(id, operatorConfig, incomingFragmentNames, outgoingFragmentNames);
                case "TargetOp" -> parseTargetOperator(id, operatorConfig, incomingFragmentNames);
                case "FragmentOp" -> parseFragmentOperator(id, incomingFragmentNames, outgoingFragmentNames);
                case "RenameOp" -> parseRenameOperator(id, operatorConfig, incomingFragmentNames, outgoingFragmentNames);
                case "JoinOp" ->  parseJoinOperator(id, operatorConfig, incomingFragmentNames, outgoingFragmentNames);

                default -> throw new IllegalStateException(
                        "Unexpected operator type: " + node.getJSONObject("operator").getString("type"));
            };
            operators.add(op);
        }

        return operators;
    }

    private Operator parseRenameOperator(String id, JSONObject operatorConfig, Set<String> inputFragments, Set<String> outputFragments) {
        if (operatorConfig.has("alias")) {
            return new RenameOperator(id, inputFragments, outputFragments, operatorConfig.getString("alias"));
        }

        return null;
    }

    /**
     * Parses the JoinOperator description
     *
     * @param id             id of the operator (for debugging purposes)
     * @param operatorConfig configuration of the operator as JSON
     * @param inputFragments  fragments the operator reads from
     * @param outputFragments fragments the operator writes to
     * @return an instance of JoinOperator: Theta, Natural or LeftJoin
     */
    private Operator parseJoinOperator(String id, JSONObject operatorConfig, Set<String> inputFragments, Set<String> outputFragments) {
        String joinType = operatorConfig.getString("join_type");

        return switch (joinType) {
            case "NaturalJoin" -> new NaturalJoinOperator(id, inputFragments, outputFragments);
            case "InnerJoin" -> {

                JoinCondition jc = parseJoinCondition(operatorConfig);
                yield new ThetaJoinOperator(id, inputFragments, outputFragments, jc);
            }
            default -> throw new IllegalArgumentException("Unexpected join type: " + joinType);
        };
    }

    private JoinCondition parseJoinCondition(JSONObject joinconditionJSON) {
        JSONArray attrPairs = joinconditionJSON.getJSONArray("left_right_attr_pairs");
        Map<String, String> attrPairsMap = new HashMap<>();
        for (int i = 0; i < attrPairs.length(); i++) {
            JSONArray pair = attrPairs.getJSONArray(i);
            String left = pair.getString(0);
            String right = pair.getString(1);

            attrPairsMap.put(left, right);
        }

        String predicateType = joinconditionJSON.getString("predicate_type");

        return switch (predicateType) {
            case "Equal" -> new EqualityJoinCondition(attrPairsMap);
            default -> throw new IllegalArgumentException("Unexpected join condition type: " + predicateType);

        };

    }

    /**
     * Parses the Fragment operator. The output fragment of this operator is
     * determined by the configuration of the operator, not outside
     *
     * @param id             id of the operator (for debugging purposes)
     * @param inputFragments  fragments the operator reads from
     * @param outputFragments fragments the operator writes to
     * @return an instance of FragmentOperator
     */
    private Operator parseFragmentOperator(String id, Set<String> inputFragments, Set<String> outputFragments) {
        return new FragmenterOperator(id, inputFragments, outputFragments, new CopyFragmentFunction(inputFragments, outputFragments));
    }

    /**
     * Parses the Target operator
     *
     * @param id       id of the operator
     * @param config   configuration of the operator as JSON
     * @param inputFragments the input fragments this operator works on
     * @return an instance of TargetOperator
     */
    private Operator parseTargetOperator(String id, JSONObject config, Set<String> inputFragments) {
        return new FlinkTargetOperator(id, inputFragments, "?serialized_output", config);
    }

    /**
     * Parses the Serializer operator
     *
     * @param id             id of the operator (for debugging purposes)
     * @param operatorConfig configuration of the operator as JSON
     * @param inputFragments  fragments the operator reads from
     * @param outputFragments fragments the operator writes to
     * @return an instance of SerializeOperator
     */
    private Operator parseSerializerOperator(String id, JSONObject operatorConfig, Set<String> inputFragments, Set<String> outputFragments) {
        String template = operatorConfig.getString("template");
        String language = operatorConfig.getString("format");

        // check the object of the template for language tags
        // break the template down into parts
        List<String> parts = Arrays.stream(template.split(" ")).toList();
        if (parts.size() > 1) {
            String object = parts.get(2);

            // extract the language tag
            Matcher m = this.languagePattern.matcher(object);
            if (m.find()) {
                String languageTag = m.group(1);
                Matcher allowedLanguageMatcher = this.allowedLanguagesPattern.matcher(languageTag);
                if (!allowedLanguageMatcher.find()) {
                    throw new MappingException("Forbidden language tag '" + languageTag + "'!");
                }
            }
        }


        return new TemplateSerializer(id, inputFragments, outputFragments, template, TargetOperator.TARGET_VARIABLE);
    }

    /**
     * Parses the Project operator
     *
     * @param id             id of the operator (for debugging purposes)
     * @param operatorConfig configuration of the operator as JSON
     * @param inputFragments       fragments the operator reads from
     * @param outputFragments      fragments the operator writes to
     * @return an instance of ProjectOperator
     */
    private Operator parseProjectOperator(String id, JSONObject operatorConfig, Set<String> inputFragments, Set<String> outputFragments) {
        JSONArray variablesArr = operatorConfig.getJSONArray("projection_attributes");
        List<String> variables = variablesArr.toList().stream()
                .map(Object::toString)
                .toList();

        return new ProjectOperator(id, inputFragments, outputFragments, variables);
    }

    private Access parseFileAccess(JSONObject operatorConfig) {
        JSONObject access = operatorConfig.has("access") ? operatorConfig.getJSONObject("access") : operatorConfig;
        String path = access.has("path") ? access.getString("path") : operatorConfig.has("path") ? operatorConfig.getString("path") : null;
        if (path == null) {
            throw new MappingException("File source missing 'path' in operator configuration");
        }
        // check if the file is available
        if (!new File(Paths.get(basePath, path).toString()).exists()) {
            throw new MappingException("File on path '" + path + "' does not exist!");
        }

        Compression compression;
        if (access.has("compression")) {
            compression = switch (access.getString("compression")) {
                case "http://w3id.org/rml/gzip" -> Compression.GZip;
                case "http://w3id.org/rml/zip" -> Compression.Zip;
                case "http://w3id.org/rml/tarxz" -> Compression.TarXZ;
                case "http://w3id.org/rml/targz" -> Compression.TarGZ;
                default ->
                        throw new IllegalArgumentException("Unknown value for field compression: %s".formatted(access.getString("compression")));
            };
        } else {
            compression = Compression.None;
        }
        return new LocalFileAccess(path, basePath, "", Charset.defaultCharset(), compression);
    }

    private Access parseRDBAccess(JSONObject operatorConfig) {
        JSONObject access = operatorConfig.has("access") ? operatorConfig.getJSONObject("access") : operatorConfig;
        String query;
        if (access.has("query")) {
            if (access.has("table")) {
                throw new IllegalStateException("Both query and table are defined in the RDB source operator");
            }
            query = access.getString("query");
        } else {
            String columns = "*";
            if (access.has("column")) {
                columns = access.getString("column");
            }
            query = "SELECT " + columns + " FROM " + access.getString("tableName");
        }

        String dsn = access.getString("jdbcDSN");
        DatabaseType databaseType = DatabaseType.getDBtype(access.getString("jdbcDriver"));

        if (dsn.equals("CONNECTIONDSN")) {
            // use default dsn
            dsn = "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=UTC&characterEncoding=UTF-8&useUnicode=true&autoReconnect=true&connectTimeout=5000";
        }

        String username = access.getString("username");
        String password = access.getString("password");

        return new RDBAccess(dsn, databaseType, username, password, query, "application/csv");
    }

    private Access parseWebSocketAccess(JSONObject operatorConfig) {
        JSONObject access = operatorConfig.getJSONObject("access");
        String url = access.getString("url");
        
        Map<String, String> headers = new HashMap<>();
        if (access.has("headers")) {
            JSONObject headersObj = access.getJSONObject("headers");
            for (String key : headersObj.keySet()) {
                headers.put(key, headersObj.getString(key));
            }
        }
        
        // Default timeout of 30 seconds
        Duration timeout = Duration.ofSeconds(30);
        if (access.has("timeout")) {
            timeout = Duration.ofSeconds(access.getLong("timeout"));
        }
        
        return new WebSocketAccess(url, headers, timeout);
    }

    private Access parseAccess(JSONObject operatorConfig) {
        String sourceType = operatorConfig.getString("source_type");

        return switch (sourceType) {
            case "File" -> parseFileAccess(operatorConfig);
            case "RDB" -> parseRDBAccess(operatorConfig);
            case "Websocket" -> parseWebSocketAccess(operatorConfig);
            default -> throw new IllegalStateException("Unexpected source type: " + sourceType);
        };
    }

    /**
     * Parses the Source operator
     *
     * @param id             id of the operator (for debugging purposes)
     * @param operatorConfig configuration of the operator as JSON
     * @return an instance of SourceOperator
     */
    private Operator parseSourceOperator(String id, JSONObject operatorConfig, Set<String> outputFragments) {
        JSONObject rootIteratorObj = operatorConfig.getJSONObject("root_iterator");
        if (operatorConfig.getString("source_type").equals("Kafka")) {
            return parseKafkaSource(id, operatorConfig, outputFragments, rootIteratorObj);
        }


        Object referenceFormulation = rootIteratorObj.get("reference_formulation");
        if (referenceFormulation instanceof String){
            return switch ((String) referenceFormulation) {
                case "CSVRows" -> parseCSVOperator(id, operatorConfig, outputFragments, rootIteratorObj);
                case "JSONPath" -> parseJSONOrXMLOperator(id, operatorConfig, outputFragments, rootIteratorObj, true);
                case "XMLPath" -> parseJSONOrXMLOperator(id, operatorConfig, outputFragments, rootIteratorObj, false);
                default -> throw new IllegalStateException("Unexpected reference formulation: " + referenceFormulation);
            };

        }else{
            JSONObject referenceFormulationObj = (JSONObject) referenceFormulation;
            return switch (referenceFormulationObj.keys().next()) {
                case "XMLPath" -> parseXMLOperator(id, operatorConfig, outputFragments, rootIteratorObj, referenceFormulationObj.getJSONObject("XMLPath"));
                default -> throw new IllegalStateException("Unexpected reference formulation: " + referenceFormulation);
            };
        }

    }
    private SourceOperator parseXMLOperator(final String id, final JSONObject config, final Set<String> outputFragments, final JSONObject rootIteratorObj, JSONObject referenceFormulationObj){
        JSONArray fieldsArray = rootIteratorObj.getJSONArray("fields");
        List<JSONPlanField> fields = parseFields(fieldsArray);
        List<Field> amoFields = fields.stream().map(JSONPlanField::getAMOField).toList();
        Map<String,String> namespaceMap = new HashMap<>();
        for (Object namespace: referenceFormulationObj.getJSONArray("namespaces")){
            JSONObject namespaceJSON = (JSONObject) namespace;
            String prefix = namespaceJSON.getString("prefix");
            String uri = namespaceJSON.getString("url");
            namespaceMap.put(prefix, uri);
        }

        final Access access = parseAccess(config);

        final String rootIterator = parseRootIterator(rootIteratorObj);
        return new XMLSourceOperator(
                id,
                access,
                outputFragments,
                rootIterator,
                amoFields,
                Set.of(),
                namespaceMap
        );
    }

    private SourceOperator parseJSONOrXMLOperator(final String id, final JSONObject config, final Set<String> outputFragments, final JSONObject rootIteratorObj, boolean isJSON) {
        JSONArray fieldsArray = rootIteratorObj.getJSONArray("fields");
        System.out.println(config.toString());
        List<JSONPlanField> fields = parseFields(fieldsArray);
        List<Field> amoFields = fields.stream().map(JSONPlanField::getAMOField).toList();
        final Access access = parseAccess(config);
        final String rootIterator = parseRootIterator(rootIteratorObj);

        if (isJSON) {
            return new JSONSourceOperator(
                    id,
                    access,
                    outputFragments,
                    rootIterator,
                    amoFields,
                    Set.of()
            );
        } else {
            return new XMLSourceOperator(
                    id,
                    access,
                    outputFragments,
                    rootIterator,
                    amoFields,
                    Set.of()
            );
        }
    }

    private List<JSONPlanField> parseFields(JSONArray fieldsArray) {
        List<JSONPlanField> JSONPlanFields = new ArrayList<>();
        for (int i = 0; i < fieldsArray.length(); i++) {
            JSONPlanFields.add(parseField(fieldsArray.getJSONObject(i)));
        }
        return JSONPlanFields;
    }

    private JSONPlanField parseField(JSONObject field) {
        List<JSONPlanField> subfields = parseFields(field.getJSONArray("inner_fields"));
        Object refFormulation = field.get("reference_formulation");
        String refFormulationString;
        if (refFormulation instanceof JSONObject){
            refFormulationString = ((JSONObject) refFormulation).keys().next();
        }else{
            refFormulationString = (String) refFormulation;
        }
        return new JSONPlanField(
                field.isNull("reference") ? null : field.getString("reference"),
                field.isNull("reference_formulation") ? null : refFormulationString,
                field.isNull("iterator") ? null : field.getString("iterator"),
                field.isNull("constant") ? null : field.getString("constant"),
                field.isNull("alias") ? null : field.getString("alias"),
                subfields);
    }


    private String parseRootIterator(JSONObject config) {
        return config.getString("reference");
    }

    private SourceOperator parseCSVOperator(final String id, final JSONObject config, final Set<String> outputFragments, final JSONObject rootIteratorObj) {
        JSONArray fieldsArray = rootIteratorObj.getJSONArray("fields");
        final Access access = parseAccess(config);

        List<String> nulls = new ArrayList<>();
        if (config.has("nullable_vec")) {
            String nullVec =  config.getString("nullable_vec");
            nulls.addAll(Arrays.asList(nullVec.split(",")));
        }

        List<JSONPlanField> fields = parseFields(fieldsArray);

        List<Field> amoFields = fields.stream().map(JSONPlanField::getAMOField).toList();

        return new CSVSourceOperator(
                id,
                access,
                outputFragments,
                amoFields,
                nulls
        );
    }

    // TODO: does this work??
    private KafkaSourceOperator parseKafkaSource(final String id, final JSONObject config, final Set<String> outputFragments, final JSONObject rootIteratorObj) {
        String referenceFormulation = rootIteratorObj.getString("reference_formulation");

        List<JSONPlanField> JSONPlanFields = parseFields(rootIteratorObj.getJSONArray("JSONPlanFields"));

//        List<String> JSONPlanFields = new ArrayList<>();
//        if (referenceFormulation.equals("JSONPath") || referenceFormulation.equals("XMLPath")) {
//            for (Object obj : rootIteratorObj.getJSONArray("JSONPlanFields")) {
//                JSONObject field = (JSONObject) obj;
//                JSONPlanFields.add(field.getString("reference"));
//            }
//        }

        String rootIterator = parseRootIterator(rootIteratorObj);

        // Access JSONPlanFields will be filled in as records come in
        SourceOperator op = switch (referenceFormulation) {
            case "CSVRows" -> new CSVSourceOperator(id, null, outputFragments, Set.of(), Set.of()); // TODO: check...
            case "JSONPath" -> new JSONSourceOperator(id, null, outputFragments, rootIterator, Set.of(), Set.of());
            case "XMLPath" -> new  XMLSourceOperator(id, null, outputFragments, rootIterator, Set.of(), Set.of());

            default -> throw new IllegalStateException("Unexpected reference formulation: " + referenceFormulation);
        };

        return new KafkaSourceOperator(id,
                config.getString("groupId"),
                config.getString("topic"),
                config.getString("broker"),
                (DataIOSourceOperator) op,
                outputFragments
        );
    }

    record JSONPlanField(
            String reference,
            String referenceFormulation,
            String iterator,
            String constant,
            String alias,
            List<JSONPlanField> innerFields
    ) {
        public Field getAMOField() {
            List<Field> subfields = innerFields.stream().map(JSONPlanField::getAMOField).toList();

            FieldBuilder builder = Field.builder()
                    .withName(alias)
                    .withReferenceFormulation(ReferenceFormulation.valueOf(referenceFormulation))
                    .withSubfields(subfields);

            if (constant != null) {
                // a little hack to get the correct RDF node in here...
                final String mockedTriple = "<http://example.com/s> <http://example.com/p> " + constant + " .";
                final Model model = ModelFactory.createDefaultModel();
                RDFDataMgr.read(model, new ByteArrayInputStream(mockedTriple.getBytes(StandardCharsets.UTF_8)), Lang.NTRIPLES);
                Statement statement = model.listStatements().nextStatement();
                org.apache.jena.rdf.model.RDFNode object = statement.getObject();
                RDFNode rdfConstant;

                if (object.isURIResource()) {
                    rdfConstant = new IRINode(object.asResource().getURI());
                } else  {   // it is a Literal!
                    Literal litObject = object.asLiteral();
                    rdfConstant = new LiteralNode(litObject.getString(), litObject.getDatatype().getURI(), litObject.getLanguage());
                }
                builder = builder.withConstant(rdfConstant);
            }

            if (iterator != null) {
                return builder.withIterator(iterator).build();
            }

            return builder.withReference(reference).build();
        }
    }
}
