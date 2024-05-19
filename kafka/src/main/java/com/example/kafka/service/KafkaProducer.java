package com.example.kafka.service;

import com.example.kafka.message.OrderCreated;
import com.example.kafka.message.OrderDispatched;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class KafkaProducer {

    private static final String ORDER_DISPATCHED_TOPIC = "order.dispatched";
    private final KafkaTemplate<String,Object> kafkaTemplate;

    public void process(String Key, OrderDispatched orderDispatched) throws ExecutionException, InterruptedException {
        OrderDispatched orderDispatched1 = OrderDispatched.builder()
                .uuid(orderDispatched.getUuid())
                .build();
        kafkaTemplate.send(ORDER_DISPATCHED_TOPIC, Key, orderDispatched1).get();
    }
}
