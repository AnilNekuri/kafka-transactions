package com.experian.transaction.service;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
public class BookingService {

    private final static Logger logger = LoggerFactory.getLogger(BookingService.class);

    private final JdbcTemplate jdbcTemplate;

    private Producer kafkaProducer;

    public BookingService(JdbcTemplate jdbcTemplate){
        this.jdbcTemplate = jdbcTemplate;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092,localhost:39092,localhost:49092");
        props.put("acks", "all");
        props.put("transactional.id", "transactional-id");
        kafkaProducer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        kafkaProducer.initTransactions();
    }

    @Transactional
    public void book(String... persons){
        try{
            kafkaProducer.beginTransaction();
            Arrays.stream(persons).forEach((person) -> {
                        logger.info("Booking {} in as seat",person);
                        kafkaProducer.send(new ProducerRecord<>("TRAN-TOPIC", "one", person));
                        jdbcTemplate.update("insert into BOOKINGS(FIRST_NAME) values (?)", person);
                    }
            );
            kafkaProducer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            kafkaProducer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            kafkaProducer.abortTransaction();
        } catch (Exception e) {
            // For all other exceptions, just abort the transaction and try again.
            kafkaProducer.abortTransaction();
        }
    }

    public List<String> findAllBookings() {
        return jdbcTemplate.query("select FIRST_NAME from BOOKINGS",
                (rs, rowNum) -> rs.getString("FIRST_NAME"));
    }

}
