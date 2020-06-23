package com.ibm.garage.cpat.cp4i.TradeEnrichment;

import com.ibm.garage.cpat.cp4i.FinancialMessage.FinancialMessage;

import io.reactivex.Flowable;
import io.smallrye.reactive.messaging.annotations.Broadcast;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;

import org.eclipse.microprofile.reactive.messaging.Outgoing;
import org.eclipse.microprofile.reactive.messaging.Incoming;


@ApplicationScoped
public class TradeEnrichment {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TradeEnrichment.class);

    // @Incoming annotation denotes the incoming channel that we'll be reading from.
    // The @Outgoing denotes the outgoing channel that we'll be sending to.
    @Incoming("pre-trade-check")
    @Outgoing("post-trade-check")
    @Broadcast
    public Flowable<FinancialMessage> processCompliance(FinancialMessage financialMessage) {

        FinancialMessage receivedMessage = financialMessage;

        LOGGER.info("Message received from topic = {}", receivedMessage);

        if (receivedMessage.trade_enrichment && !receivedMessage.business_validation && 
            !receivedMessage.schema_validation) {
            /*
            Check whether trade_enrichment is true as well as if business (previous) is false. If so 
            it's ready to be processed. We flip the boolean value to indicate that this service has processed it.
            Considering this trade enrichment step is the last step before passing it on to Camel Reactive
            and lastly APIC.
            */
            receivedMessage.trade_enrichment = false;
            receivedMessage.business_validation = true;

            return Flowable.just(receivedMessage);
        }

        else {
            return Flowable.empty();
        }
    }
}