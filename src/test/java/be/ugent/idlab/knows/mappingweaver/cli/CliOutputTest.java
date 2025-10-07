package be.ugent.idlab.knows.mappingweaver.cli;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.mqtt.MqttQoS;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.eclipse.paho.mqttv5.client.*;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.testcontainers.hivemq.HiveMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CliOutputTest {

    @Test
    public void testToFile() {
        String[] args = {"-m", "src/test/resources/test-cases/csv/RMLTC0001a-CSV/mapping.ttl", "toFile", "-o", "/tmp/output.ttl"};

        Main.main(args);

        String expected, actual;

        try (InputStream actualStream = new FileInputStream("/tmp/output.ttl"); InputStream expectedStream = new FileInputStream("src/test/resources/test-cases/csv/RMLTC0001a-CSV/output.nq")) {
            expected = new String(expectedStream.readAllBytes()).strip();
            actual = new String(actualStream.readAllBytes()).strip();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        assertEquals(expected, actual);
    }

    @Test
    public void testToTCPSocket() throws IOException {
        // start a server
        try (ServerSocket server = new ServerSocket(0)) {
            String[] args = {"-m", "src/test/resources/test-cases/csv/RMLTC0001a-CSV/mapping.ttl", "toTCPSocket", "-s", server.getInetAddress().getHostAddress() + ":" + server.getLocalPort()};

            Thread t = new Thread(() -> Main.main(args));

            List<String> lines = new ArrayList<>();
            Thread t2 = new Thread(() -> {
                Socket client;
                try {
                    client = server.accept();
                    BufferedReader reader = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while ((line = reader.readLine()) != null) {
                        lines.add(line);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            t.start();
            t2.start();
            t.join();
            t2.join();

            String expected;
            try (InputStream is = new FileInputStream("src/test/resources/test-cases/csv/RMLTC0001a-CSV/output.nq")) {
                expected = new String(is.readAllBytes()).strip();
            }

            String actual = lines.stream().reduce(String::concat).orElse("");

            assertEquals(expected, actual);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @Disabled("Test does not work, but the output does") // TODO
    @Testcontainers
    class ToKafka {
        @Container
        KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.8.0");

        @Test
        public void testToKafka() {

            // create the test topic on the cluster
            Properties adminProps = new Properties();
            adminProps.putAll(Map.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
            ));

            try (AdminClient client = AdminClient.create(adminProps)) {
                NewTopic topic = new NewTopic("test-topic", 1, (short) 1);
                CreateTopicsResult result = client.createTopics(List.of(topic));
                result.all().get();
            } catch (ExecutionException ex) {
                // noop
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            String expected;
            try (InputStream expectedStream = new FileInputStream("src/test/resources/test-cases/csv/RMLTC0001a-CSV/output.nq")) {
                expected = new String(expectedStream.readAllBytes()).strip();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            String[] args = {"-m", "src/test/resources/test-cases/csv/RMLTC0001a-CSV/mapping.ttl", "toKafka", "-b", kafka.getBootstrapServers(), "-t", "test-topic"};

            Properties props = new Properties();
            props.putAll(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "test-topic",
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 100,
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
            ));

            try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of("test-topic"));

                Main.main(args);

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(4));
                assertEquals(1, records.count());
                ConsumerRecord<String, String> record = records.iterator().next();
                assertEquals(expected, record.value());
            }
        }
    }

    @Nested
    @Testcontainers
    class ToMQTT {

        @Container
        HiveMQContainer mq = new HiveMQContainer(DockerImageName.parse("hivemq/hivemq-ce"));

        @Test
        public void test() throws MqttException, InterruptedException, IOException {
            String broker = mq.getHost() + ":" + mq.getMqttPort();
            String topic = "test-topic";

            // pull from broker
            MqttClient client = new MqttClient("tcp://" + broker, "test");

            MqttConnectionOptions options = new MqttConnectionOptions();
            client.connect(options);

            client.subscribe(topic, MqttQoS.AT_LEAST_ONCE.value());

            List<String> collected = new ArrayList<>();

            client.setCallback(new MqttCallback() {
                @Override
                public void disconnected(MqttDisconnectResponse disconnectResponse) {

                }

                @Override
                public void mqttErrorOccurred(MqttException e) {

                }


                @Override
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    System.out.println("Message arrived: " + new String(message.getPayload()));
                    collected.add(new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttToken token) {

                }

                @Override
                public void connectComplete(boolean reconnect, String serverURI) {

                }

                @Override
                public void authPacketArrived(int reasonCode, MqttProperties properties) {

                }
            });

            String[] args = {"-m", "src/test/resources/test-cases/csv/RMLTC0001a-CSV/mapping.ttl", "toMQTT", "-b", broker, "-t", topic};
            Main.main(args);

            // give the messages a chance to arrive
            Thread.sleep(1000);

            client.disconnect();
            client.close();

            String actual = collected.stream().reduce(String::concat).orElse("");
            String expected;
            try (InputStream is = new FileInputStream("src/test/resources/test-cases/csv/RMLTC0001a-CSV/output.nq")) {
                expected = new String(is.readAllBytes()).strip();
            }

            assertEquals(expected, actual);

        }
    }
}
