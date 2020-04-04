package com.example.siehuai.kafkatesting.aop;

import com.example.siehuai.kafkatesting.repository.Person;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Aspect
@Component
public class KafkaTransactionAop {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Around("@annotation(javax.transaction.Transactional)")
    public Object surroundInTransaction(ProceedingJoinPoint pjp) throws Throwable {
        Object publishedObject = pjp.proceed();
        this.kafkaTemplate.executeInTransaction(t -> {
            Person person = (Person) publishedObject;
            t.send("person", person.getName());
            return person;
        });
        return publishedObject;
    }

}
