package com.example.kafka.handler;

import com.example.kafka.message.OrderCreated;
import com.example.kafka.message.OrderDispatched;
import com.example.kafka.service.KafkaProducer;
import com.example.kafka.service.KafkaService;
import com.example.kafka.service.PojoKafka;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class kafkaHandler {

    private final KafkaService kafkaService;
    private final PojoKafka pojoKafka;
    private final KafkaProducer kafkaProducer;

    @KafkaListener(
            id = "OrderConsumerClient",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "concurrentKafkaListenerContainerFactory"
    )
    public void listen(@Header(KafkaHeaders.RECEIVED_PARTITION) Integer partition, @Header(KafkaHeaders.RECEIVED_KEY) String Key, @Payload String payload)
    {
        log.info("Received message partition " + partition + "key value :"+ Key + "payload is :" +payload);
        kafkaService.process(payload);
    }


    @KafkaListener(
            id = "OrderConsumerClient1",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "concurrentKafkaListenerContainerFactory"
    )
    public void listen1(OrderCreated orderCreated)
    {
        log.info("Received message OrderCreated object:"+ orderCreated.getItem());
        pojoKafka.process(orderCreated.getUuid(), orderCreated.getItem());
    }

    @KafkaListener(
            id = "OrderConsumerClient1",
            topics = "order.created",
            groupId = "dispatch.order.created.consumer",
            containerFactory = "concurrentKafkaListenerContainerFactory"
    )
    public void listen2(@Header(KafkaHeaders.RECEIVED_KEY) String Key, @Payload  OrderDispatched orderCreated)
    {
        log.info("Received message OrderCreated object:"+ orderCreated.getUuid());
        try {
            kafkaProducer.process(Key, orderCreated);
        }
        catch (Exception ex)
        {
            log.error("processing failure:", ex);
        }
    }

}
