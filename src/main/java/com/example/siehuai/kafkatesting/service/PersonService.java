package com.example.siehuai.kafkatesting.service;

import com.example.siehuai.kafkatesting.repository.Person;
import com.example.siehuai.kafkatesting.repository.PersonRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;

@Service
public class PersonService {
    @Autowired
    PersonRepository personRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;


    @Transactional
    public Person savePerson(Person person) {
        return this.kafkaTemplate.executeInTransaction(t -> {
            Person save = this.personRepository.save(person);
            t.send("person", person.getName());
            throw new RuntimeException();
        });
    }

}
