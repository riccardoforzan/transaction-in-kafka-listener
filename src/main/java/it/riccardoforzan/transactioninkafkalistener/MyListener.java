package it.riccardoforzan.transactioninkafkalistener;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@AllArgsConstructor
@Slf4j
public class MyListener {

  private final MyService myService;

  @KafkaListener(
    topics = "test",
    batch = "true"
  )
  public void listen(List<String> strings) {
    log.info("Received: {}", strings);
    List<MyDocument> list = strings
                              .stream()
                              .map(e -> new MyDocument(ObjectId.get(), e))
                              .toList();
    myService.transactionalBulkSave(list);
  }
}
