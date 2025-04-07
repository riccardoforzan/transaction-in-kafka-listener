package it.riccardoforzan.transactioninkafkalistener;

import org.bson.types.ObjectId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(
  classes = {
    MyConfiguration.class,
    MyService.class,
    MyRepository.class,
  }
)
@EnableAutoConfiguration
@Import(TestcontainersConfiguration.class)
class MyServiceTest {

  @Autowired
  MongoOperations mongoOperations;
  @Autowired
  MyService myService;

  @AfterEach
  void tearDown() {
    mongoOperations.remove(new Query(), MyDocument.class);
  }

  @Test
  void shouldInsertDocuments() {
    List<String> strings = List.of("1", "2", "3");

    List<MyDocument> list = strings
                              .stream()
                              .map(e -> new MyDocument(ObjectId.get(), e))
                              .toList();

    myService.transactionalBulkSave(list);

    await().atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(mongoOperations.findAll(MyDocument.class))
                             .hasSize(3)
                             .map(MyDocument::value)
                             .containsExactlyInAnyOrderElementsOf(strings));
  }

  @Test
  void shouldTolerateDuplicates() {
    List<String> strings = List.of("1", "2", "3", "1");

    List<MyDocument> list = strings
                              .stream()
                              .map(e -> new MyDocument(ObjectId.get(), e))
                              .toList();

    myService.transactionalBulkSave(list);

    await().atMost(10, TimeUnit.SECONDS)
      .untilAsserted(() -> assertThat(mongoOperations.findAll(MyDocument.class))
                             .hasSize(3)
                             .map(MyDocument::value)
                             .containsExactlyInAnyOrderElementsOf(strings));
  }

}
