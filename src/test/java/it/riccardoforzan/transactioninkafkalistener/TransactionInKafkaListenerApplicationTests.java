package it.riccardoforzan.transactioninkafkalistener;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
  classes = {
    MyConfiguration.class,
    MyListener.class,
    MyService.class,
    MyRepository.class,
    TransactionInKafkaListenerApplicationTests.TestConfig.class
  },
  properties = {
    "spring.kafka.consumer.group-id=test",
    "spring.kafka.producer.batch-size=500"
  }
)
@EnableAutoConfiguration
@Import(TestcontainersConfiguration.class)
class TransactionInKafkaListenerApplicationTests {

  @Autowired
  MongoOperations mongoOperations;
  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;

  @AfterEach
  void tearDown() {
    mongoOperations.remove(new Query(), MyDocument.class);
  }

  @Test
  void shouldInsert() {
    kafkaTemplate.send("test", UUID
                                 .randomUUID()
                                 .toString(), "1");
    kafkaTemplate.send("test", UUID
                                 .randomUUID()
                                 .toString(), "2");

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(mongoOperations.findAll(MyDocument.class)).hasSize(2));
  }

  @Test
  void shouldTolerateDuplicates() {
    kafkaTemplate.send("test", UUID
                                 .randomUUID()
                                 .toString(), "1");

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(mongoOperations.findAll(MyDocument.class)).hasSize(1));

    kafkaTemplate.send("test", UUID
                                 .randomUUID()
                                 .toString(), "1");
    kafkaTemplate.send("test", UUID
                                 .randomUUID()
                                 .toString(), "2");

    await()
      .atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(mongoOperations.findAll(MyDocument.class)).hasSize(2));
  }

  @Configuration(proxyBeanMethods = false)
  static class TestConfig {
    @Bean
    NewTopic newTopics() {
      return new NewTopic("test", 1, (short) 1);
    }
  }

}
