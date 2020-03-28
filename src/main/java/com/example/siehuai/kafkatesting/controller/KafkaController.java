package com.example.siehuai.kafkatesting.controller;

import com.example.siehuai.kafkatesting.kafka.Producer;
import com.example.siehuai.kafkatesting.repository.Person;
import com.example.siehuai.kafkatesting.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaController {

    @Autowired
    Producer producer;

    @Autowired
    PersonService personService;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping(value = "/publish")
    public String publish(@RequestParam("value") String value, @RequestParam("key") String key) {
        if (key.isEmpty()) {
            producer.sendMessage(value);
            return "OK";
        }

        producer.sendMessageWithKey(value, key);
        return "OK with key";
    }

    @GetMapping(value = "/publishPerson")
    public Integer publishPerson(@RequestParam("value") String name) {
        Person person = new Person();
        person.setName(name);
        person.setCity("KL");
        Person savePerson = personService.savePerson(person);
        return savePerson.getId();
    }
}
