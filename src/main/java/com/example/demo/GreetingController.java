package com.example.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class GreetingController {

    private final static String TOPIC = "barsss";
    private final static String BOOTSTRAP_SERVERS =
            "62.210.244.86:59093";

    private static final String template = "Hello, %s!";
    private final AtomicLong counter = new AtomicLong();

    @RequestMapping("/greeting")
    public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) {
        System.out.println("on a recu la requete");
        try {
            runProducer(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new Greeting(counter.incrementAndGet(),
                String.format(template, name));
    }

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        return new KafkaProducer<String, String>(props);
    }

    static void runProducer(final String name) throws Exception {
        final Producer<String, String> producer = createProducer();
        System.out.println("on a créé le producer");
        long time = System.currentTimeMillis();

        try {
            //  for (long index = time; index < time + sendMessageCount; index++) {
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(TOPIC, "Hello Mom " + name);

            producer.send(record);
            System.out.println("on a envoyé le message");
            producer.close();

            //long elapsedTime = System.currentTimeMillis() - time;
//            System.out.printf("sent record(key=%s value=%s) " +
//                            "meta(partition=%d, offset=%d) time=%d\n",
//                    record.key(), record.value(), metadata.partition(),
//                    metadata.offset(), elapsedTime);
//            System.out.println("on a poussé dans kafka");
            //  }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}