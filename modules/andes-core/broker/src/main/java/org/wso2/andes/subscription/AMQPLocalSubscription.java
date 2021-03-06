/*
 * Copyright (c) 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.subscription;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.AMQException;
import org.wso2.andes.amqp.AMQPUtils;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.*;
import org.wso2.andes.kernel.disruptor.inbound.InboundSubscriptionEvent;
import org.wso2.andes.server.AMQChannel;
import org.wso2.andes.server.ClusterResourceHolder;
import org.wso2.andes.server.exchange.DirectExchange;
import org.wso2.andes.server.message.AMQMessage;
import org.wso2.andes.server.queue.AMQQueue;
import org.wso2.andes.server.queue.QueueEntry;
import org.wso2.andes.server.subscription.Subscription;
import org.wso2.andes.server.subscription.SubscriptionImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class represents a AMQP subscription locally created
 * This class has info and methods to deal with qpid AMQP transports and
 * send messages to the subscription
 */
public class AMQPLocalSubscription extends InboundSubscriptionEvent {

    private static Log log = LogFactory.getLog(AMQPLocalSubscription.class);
    //AMQP transport channel subscriber is dealing with
    AMQChannel channel = null;
    //internal qpid queue subscription is bound to
    private AMQQueue amqQueue;
    //internal qpid subscription
    private Subscription amqpSubscription;
    /**
     * Whether subscription is bound to topic or not
     */
    private boolean isBoundToTopic;
    /**
     * Whether subscription is durable or not
     */
    private boolean isDurable;
    /**
     * OnflightMessageTracker stores message information, using it to get message information
     */
    private OnflightMessageTracker onflightMessageTracker;
    /**
     * List of Delivery Rules to evaluate
     */
    private List<DeliveryRule> deliveryRulesList = new ArrayList<DeliveryRule>();

    /**
     * Count sent but not acknowledged message count for channel of the subscriber
     */
    private AtomicInteger unAckedMsgCount = new AtomicInteger(0);

    private Integer maxNumberOfUnAckedMessages = 100000;

    /**
     * Map to track messages being sent <message id, MsgData reference>
     */
    private final ConcurrentHashMap<Long, MessageData> messageSendingTracker
            = new ConcurrentHashMap<Long, MessageData>();

    public AMQPLocalSubscription(AMQQueue amqQueue, Subscription amqpSubscription, String subscriptionID, String destination,
                                 boolean isBoundToTopic, boolean isExclusive, boolean isDurable,
                                 String subscribedNode, long subscribeTime, String targetQueue, String targetQueueOwner,
                                 String targetQueueBoundExchange, String targetQueueBoundExchangeType,
                                 Short isTargetQueueBoundExchangeAutoDeletable, boolean hasExternalSubscriptions) {

        super(subscriptionID, destination, isBoundToTopic, isExclusive, isDurable, subscribedNode, subscribeTime, targetQueue, targetQueueOwner,
                targetQueueBoundExchange, targetQueueBoundExchangeType, isTargetQueueBoundExchangeAutoDeletable, hasExternalSubscriptions);

        this.maxNumberOfUnAckedMessages = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_ACK_HANDLING_MAX_UNACKED_MESSAGES);

        setSubscriptionType(SubscriptionType.AMQP);

        this.amqQueue = amqQueue;
        this.amqpSubscription = amqpSubscription;
        this.isBoundToTopic = isBoundToTopic;
        this.isDurable = isDurable;
        onflightMessageTracker = OnflightMessageTracker.getInstance();

        if (amqpSubscription != null && amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
            channel = ((SubscriptionImpl.AckSubscription) amqpSubscription).getChannel();
            initializeDeliveryRules();
        }
    }

    /**
     * Initializing Delivery Rules
     */
    private void initializeDeliveryRules() {

        //checking counting delivery rule
        
        if (  (! isBoundToTopic) || isDurable){ //evaluate this only for queues and durable subscriptions
            deliveryRulesList.add(new MaximumNumOfDeliveryRule(channel));
        }
        // NOTE: Feature Message Expiration moved to a future release
//        //checking message expiration deliver rule
//        deliveryRulesList.add(new MessageExpiredRule());

        //checking message purged delivery rule
        deliveryRulesList.add(new MessagePurgeRule());
        //checking has interest delivery rule
        deliveryRulesList.add(new HasInterestRule(amqpSubscription));
        //checking no local delivery rule
        deliveryRulesList.add(new NoLocalRule(amqpSubscription, channel));
    }

    public boolean isActive() {
        return amqpSubscription.isActive();
    }

    @Override
    public UUID getChannelID() {
        return channel.getId();
    }

    @Override
    public boolean hasRoomToAcceptMessages() {

        int notAcknowledgedMsgCount = unAckedMsgCount.get();
        if (notAcknowledgedMsgCount < maxNumberOfUnAckedMessages) {
            return true;
        } else {

            if (log.isDebugEnabled()) {
                log.debug(
                        "Not selected. Too much pending acks, subscription = " + this + " pending count =" +
                                (notAcknowledgedMsgCount));
            }

            return false;
        }
    }

    @Override
    public void ackReceived(long messageID) {
        messageSendingTracker.remove(messageID);
        unAckedMsgCount.decrementAndGet();
    }

    @Override
    public void msgRejectReceived(long messageID) {
        messageSendingTracker.remove(messageID);
        unAckedMsgCount.decrementAndGet();
    }

    @Override
    public void close() {
        messageSendingTracker.clear();
        unAckedMsgCount.set(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendMessageToSubscriber(AndesMessageMetadata messageMetadata, AndesContent content)
            throws AndesException {
        AMQMessage message = AMQPUtils.getAMQMessageForDelivery(messageMetadata, content);
        sendAMQMessageToSubscriber(message);
    }

    /**
     * send message to the internal subscription
     *
     * @param message      message to send
     * @throws AndesException
     */
    private void sendAMQMessageToSubscriber(AMQMessage message) throws AndesException {
        QueueEntry messageToSend = AMQPUtils.convertAMQMessageToQueueEntry(message, amqQueue);

        if (evaluateDeliveryRules(messageToSend)) {

            onflightMessageTracker.setMessageStatus(MessageStatus.DELIVERY_OK, message.getMessageId());

            sendQueueEntryToSubscriber(messageToSend);
        } else {
            //Set message status to reject
            onflightMessageTracker.setMessageStatus(MessageStatus.DELIVERY_REJECT, message.getMessageId());
            /**
             * Message tracker rejected this message from sending. Hence moving
             * to dead letter channel
             */
            String destinationQueue = message.getMessageMetaData().getMessagePublishInfo().getRoutingKey().toString();
            // Move message to DLC
            // All the Queues and Durable Topics related messages are adding to DLC
            if (!isBoundToTopic || isDurable){
                messageSendingTracker.remove(message.getMessageId());
                MessagingEngine.getInstance().moveMessageToDeadLetterChannel(message.getMessageId(), destinationQueue);
            }
        }
    }

    /**
     * Evaluating Delivery rules before sending the messages
     *
     * @param message AMQ Message
     * @return IsOKToDelivery
     * @throws AndesException
     */
    private boolean evaluateDeliveryRules(QueueEntry message) throws AndesException {
        boolean isOKToDelivery = true;

        for (DeliveryRule element : deliveryRulesList) {
            if (!element.evaluate(message)) {
                isOKToDelivery = false;
                break;
            }
        }
        return isOKToDelivery;
    }

    /**
     * validate routing keys of messages and send
     *
     * @param message message to send
     * @throws AndesException
     */
    public void sendQueueEntryToSubscriber(QueueEntry message) throws AndesException {
        sendMessage(message);
    }


    /**
     * Stamp a message as sent. This method also evaluate if the
     * message is being redelivered
     *
     * @param messageID id of the message
     * @return if message is redelivered
     */
    private boolean addMessageToSendingTracker(long messageID) {

        if (log.isDebugEnabled()) {
            log.debug("Adding message to sending tracker channel id = " + getChannelID() + " message id = "
                    + messageID);
        }

        MessageData messageData = messageSendingTracker.get(messageID);

        if (null == messageData) {
            messageData = OnflightMessageTracker.getInstance().getTrackingData(messageID);
            messageSendingTracker.put(messageID, messageData);
        }
        // increase delivery count
        int numOfCurrentDeliveries = messageData.incrementDeliveryCount(getChannelID());


        if (log.isDebugEnabled()) {
            log.debug("Number of current deliveries for message id= " + messageID + " to Channel " + getChannelID()
                    + " is " + numOfCurrentDeliveries);
        }

        //check if this is a redelivered message
        return  messageData.isRedelivered(getChannelID());
    }

    /**
     * write message to channel
     *
     * @param queueEntry message to send
     * @throws AndesException
     */
    private void sendMessage(QueueEntry queueEntry) throws AndesException {

        String msgHeaderStringID = "";
        Long messageNumber = null;

        if (queueEntry != null) {
            msgHeaderStringID = (String) queueEntry.getMessageHeader().
                    getHeader("msgID");
            messageNumber = queueEntry.getMessage().getMessageNumber();
        }

        try {

            //record message as sent to this subscriber (channel)
            boolean isRedelivery = addMessageToSendingTracker(messageNumber);
            //set redelivery header
            if(isRedelivery) {
                queueEntry.setRedelivered();
                onflightMessageTracker.setMessageStatus(MessageStatus.RESENT, messageNumber);
            } else {
                onflightMessageTracker.setMessageStatus(MessageStatus.SENT, messageNumber);
            }
            unAckedMsgCount.incrementAndGet();

            if (amqpSubscription instanceof SubscriptionImpl.AckSubscription) {
                //this check is needed to detect if subscription has suddenly closed
                if (log.isDebugEnabled()) {
                    log.debug("TRACING>> QDW- sent queue/durable topic message " +
                            msgHeaderStringID + " messageID-" + messageNumber + "-to " +
                            "subscription " + amqpSubscription);
                }
                amqpSubscription.send(queueEntry);
            } else {
                throw new AndesException("Unexpected Subscription type for message with ID : " + msgHeaderStringID);
            }
        } catch (AMQException e) {
            //TODO: we need to remove from sending tracker if we could not send

            // The error is not logged here since this will be caught safely higher up in the execution plan :
            // MessageFlusher.deliverAsynchronously. If we have more context, its better to log here too,
            // but since this is a general explanation of many possible errors, no point in logging at this state.
            throw new AndesException("Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        } catch (AndesException e) {
            throw new AndesException("Error occurred while delivering message with ID : " + msgHeaderStringID, e);
        }
    }


    public boolean equals(Object o) {
        if (o instanceof AMQPLocalSubscription) {
            AMQPLocalSubscription c = (AMQPLocalSubscription) o;
            if (this.subscriptionID.equals(c.subscriptionID) &&
                    this.getSubscribedNode().equals(c.getSubscribedNode()) &&
                    this.targetQueue.equals(c.targetQueue) &&
                    this.targetQueueBoundExchange.equals(c.targetQueueBoundExchange)) {
                return true;
            }
        }
        return false;
    }

    public int hashCode() {
        return new HashCodeBuilder(17, 31).
                append(subscriptionID).
                append(getSubscribedNode()).
                append(targetQueue).
                append(targetQueueBoundExchange).
                toHashCode();
    }
}
