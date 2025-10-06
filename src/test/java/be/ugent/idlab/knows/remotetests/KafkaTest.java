package be.ugent.idlab.knows.remotetests;

import be.ugent.idlab.knows.amo.blocks.MappingTuple;
import be.ugent.idlab.knows.amo.blocks.SolutionMapping;
import be.ugent.idlab.knows.amo.blocks.nodes.LiteralNode;
import be.ugent.idlab.knows.amo.operators.source.SourceOperator;
import be.ugent.idlab.knows.cores.TestCore;
import be.ugent.idlab.knows.mappingLoom.ITranslator;
import be.ugent.idlab.knows.mappingplan.GraphOpVisitor;
import be.ugent.idlab.knows.mappingplan.MappingPlan;
import be.ugent.idlab.knows.mappingplan.OperatorGraph;
import be.ugent.idlab.knows.utilities.GraphVisitorCustomTarget;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
@Disabled("Old RML is not supported in MappingLoom-rs, there are some issues to be fixed...")
public class KafkaTest extends TestCore {

    @Container
    KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

    @Test
    public void csvTest() throws Exception {
        // send the file to Kafka
        String file = FileUtils.readFileToString(new File("src/test/resources/kafka/csv/input.csv"), Charset.defaultCharset());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(this.producerProperties())) {
            producer.send(new ProducerRecord<>("demo", "input.csv", file)).get();
        }

        String planTTL = Files.readString(Paths.get("src/test/resources/kafka/csv/mapping.ttl"));
        planTTL = planTTL.replace("localhost:9092", kafka.getBootstrapServers());
        String planJson = ITranslator.getInstance().translate_to_document(planTTL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MappingPlan plan = MappingPlan.fromString(env, planJson, "src/test/resources/kafka/csv");
        SourceOperator op = (SourceOperator) plan.getOperatorGraph().getOperators().getFirst();
        runKafkaTest(op);
    }

    @Test
    public void jsonTest() throws Exception {
        String file = FileUtils.readFileToString(new File("src/test/resources/kafka/json/input.json"), Charset.defaultCharset());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(this.producerProperties())) {
            var metadata = producer.send(new ProducerRecord<>("demo", "input.json", file)).get();
            System.out.println(metadata);
        }

        String planTTL = Files.readString(Paths.get("src/test/resources/kafka/json/mapping.ttl"));
        planTTL = planTTL.replace("localhost:9092", kafka.getBootstrapServers());
        String planJson = ITranslator.getInstance().translate_to_document(planTTL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MappingPlan plan = MappingPlan.fromString(env, planJson, "src/test/resources/kafka/json");
        SourceOperator op = (SourceOperator) plan.getOperatorGraph().getOperators().getFirst();
        runKafkaTest(op);
    }

    @Test
    public void xmlTest() throws Exception {
        String file = FileUtils.readFileToString(new File("src/test/resources/kafka/xml/input.xml"), Charset.defaultCharset());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(this.producerProperties())) {
            producer.send(new ProducerRecord<>("demo", file, file)).get();
        }

        String planTTL = Files.readString(Paths.get("src/test/resources/kafka/xml/mapping.ttl"));
        planTTL = planTTL.replace("localhost:9092", kafka.getBootstrapServers());
        String planJson = ITranslator.getInstance().translate_to_document(planTTL);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MappingPlan plan = MappingPlan.fromString(env, planJson, "src/test/resources/kafka/xml");
        SourceOperator op = (SourceOperator) plan.getOperatorGraph().getOperators().getFirst();
        runKafkaTest(op);
    }

    private void runKafkaTest(SourceOperator op) throws Exception {
        MappingTuple expected = new MappingTuple();
        expected.addSolutionMap("default", new SolutionMapping() {{
            put("Name", new LiteralNode("Venus"));
        }});

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OperatorGraph graph = new OperatorGraph(List.of(op), new boolean[][]{{false}});

        GraphOpVisitor visitor = new GraphVisitorCustomTarget(env, graph, "target");
        visitor.visitSource(op);

        MappingTuple actual = visitor.getStreamCache().get(op).executeAndCollect(1).getFirst().getValue();

        assertEquals(expected, actual);
    }

    private Properties producerProperties() {
        Properties props = new Properties();
        props.putAll(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ));

        return props;
    }
}
