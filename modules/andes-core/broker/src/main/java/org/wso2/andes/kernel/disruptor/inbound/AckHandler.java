/*
 * Copyright (c) 2014-2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.andes.kernel.disruptor.inbound;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.disruptor.BatchEventHandler;
import org.wso2.andes.store.AndesTranactionRollbackException;
import org.wso2.andes.store.FailureObservingStoreManager;
import org.wso2.andes.store.HealthAwareStore;
import org.wso2.andes.store.StoreHealthListener;
import org.wso2.andes.subscription.SubscriptionStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Acknowledgement Handler for the Disruptor based inbound event handling.
 * This handler processes acknowledgements received from clients and updates Andes.
 */
public class AckHandler implements BatchEventHandler, StoreHealthListener {

    private static Log log = LogFactory.getLog(AckHandler.class);
    
    private final MessagingEngine messagingEngine;
    
    /**
     * Indicates and provides a barrier if messages stores become offline.
     * marked as volatile since this value could be set from a different thread
     * (other than those of disrupter)
     */
    private volatile SettableFuture<Boolean> messageStoresUnavailable;
    
    /**
     * Keeps message meta-data that needs to be removed from the message store.
     */
    List<AndesRemovableMetadata> removableMetadata;
    
    AckHandler(MessagingEngine messagingEngine) {
        this.messagingEngine = messagingEngine;
        this.messageStoresUnavailable = null;
        this.removableMetadata = new ArrayList<>();
        FailureObservingStoreManager.registerStoreHealthListener(this);
    }

    @Override
    public void onEvent(final List<InboundEventContainer> eventList) throws Exception {
        if (log.isTraceEnabled()) {
            StringBuilder messageIDsString = new StringBuilder();
            for (InboundEventContainer inboundEvent : eventList) {
                messageIDsString.append(inboundEvent.ackData.getMessageID()).append(" , ");
            }
            log.trace(eventList.size() + " messages received : " + messageIDsString);
        }
        if (log.isDebugEnabled()) {
            log.debug(eventList.size() + " acknowledgements received from disruptor.");
        }

        try {
            ackReceived(eventList);
        } catch (AndesException e) {
            // Log the AndesException since there is no point in passing the exception to Disruptor
            log.error("Error occurred while processing acknowledgements ", e);
        }
    }

    /**
     * Updates the state of Andes and deletes relevant messages. (For topics
     * message deletion will happen only when
     * all the clients acknowledges)
     * 
     * @param eventList
     *            inboundEvent list
     */
    public void ackReceived(final List<InboundEventContainer> eventList) throws AndesException {
        
        for (InboundEventContainer event : eventList) {

            AndesAckData ack = event.ackData;
            // For topics message is shared. If all acknowledgements are received only we should remove message
            boolean deleteMessage = OnflightMessageTracker.getInstance()
                    .handleAckReceived(ack.getChannelID(), ack.getMessageID());
            if (deleteMessage) {
                if (log.isDebugEnabled()) {
                    log.debug("Ok to delete message id " + ack.getMessageID());
                }
                removableMetadata.add(new AndesRemovableMetadata(ack.getMessageID(), ack.getDestination(),
                        ack.getMsgStorageDestination()));
            }

            LocalSubscription subscription = AndesContext.getInstance().getSubscriptionStore()
                    .getLocalSubscriptionForChannelId(ack.getChannelID());
            subscription.ackReceived(ack.getMessageID());
            event.clear();
        }

        /*
         * Checks for the message store availability if its not available
         * Ack handler needs to await until message store becomes available
         */
        if (messageStoresUnavailable != null) {
            try {

                log.info("Message store has become unavailable therefore waiting until store becomes available");
                messageStoresUnavailable.get();
                log.info("Message store became available. resuming ack hander");
                messageStoresUnavailable = null; // we are passing the blockade
                                                 // (therefore clear the it).
            } catch (InterruptedException e) {
                throw new AndesException("Thread interrupted while waiting for message stores to come online", e);
            } catch (ExecutionException e) {
                throw new AndesException("Error occured while waiting for message stores to come online", e);
            }
        }

        try {
            messagingEngine.deleteMessages(removableMetadata, false);
            OnflightMessageTracker.getInstance().updateMessageDeliveryInSlot(removableMetadata);

            if (log.isTraceEnabled()) {
                StringBuilder messageIDsString = new StringBuilder();
                for (AndesRemovableMetadata metadata : removableMetadata) {
                    messageIDsString.append(metadata.getMessageID()).append(" , ");
                }
                log.trace(eventList.size() + " message ok to remove : " + messageIDsString);
            }
            removableMetadata.clear();
        } catch (AndesTranactionRollbackException txrollback) {
            log.warn(
               String.format("unable to delete messages, due to trasaction roll back : %d, "
                             + "operation will be attempted again",
                             removableMetadata.size()));
        } catch (AndesException ex) {
            log.warn(
               String.format("unable to delete messages, probably due to errors in message stores."
                             + "messages count : %d, operation will be attempted again",
                             removableMetadata.size()));
            throw ex;
        }
    }

    /**
     * {@inheritDoc}
     * <p>
     * Creates a {@link SettableFuture} indicating message store became offline.
     */
    @Override
    public void storeNonOperational(HealthAwareStore store, Exception ex) {
        log.info(String.format("Message store became not operational. messages to delete : %d",
                               removableMetadata.size()));
        messageStoresUnavailable = SettableFuture.create();
    }

    /**
     * {@inheritDoc}
     * <p>
     * Sets a value for {@link SettableFuture} indicating message store became
     * online.
     */
    @Override
    public void storeOperational(HealthAwareStore store) {
        log.info(String.format("Message store became operational. messages to delete : %d",
                               removableMetadata.size()));
        messageStoresUnavailable.set(false);
    }
}
