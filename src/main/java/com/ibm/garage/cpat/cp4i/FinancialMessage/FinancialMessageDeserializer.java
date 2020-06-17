package com.ibm.garage.cpat.cp4i.FinancialMessage;

import io.quarkus.kafka.client.serialization.ObjectMapperDeserializer;


public class FinancialMessageDeserializer extends ObjectMapperDeserializer<FinancialMessage>{
    public FinancialMessageDeserializer(){
        // pass the class to the parent.
        super(FinancialMessage.class);
    }
}