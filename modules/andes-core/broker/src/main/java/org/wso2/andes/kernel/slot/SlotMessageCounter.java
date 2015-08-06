/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.andes.kernel.slot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.AndesMessage;
import org.wso2.andes.kernel.AndesMessageMetadata;
import org.wso2.andes.kernel.MessagingEngine;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is responsible of counting messages in a slot for each queue
 */
public class SlotMessageCounter {

    private ConcurrentHashMap<String, Slot> queueToSlotMap = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Long> slotTimeOutMap = new ConcurrentHashMap<>();
    /**
     * Timeout in milliseconds for messages in the slot. When this timeout is exceeded slot will be
     * submitted to the coordinator
     */
    private Long timeOutForMessagesInQueue;
    private Timer submitSlotToCoordinatorTimer = new Timer();
    private Log log = LogFactory.getLog(SlotMessageCounter.class);
    private static SlotMessageCounter slotMessageCounter = new SlotMessageCounter();
    private Integer slotWindowSize;
    private long currentSlotDeleteSafeZone;

    /**
     * Keep track of how many update loops
     * are skipped without messages.
     */
    private int slotSubmitLoopSkipCount;

    private SlotCoordinator slotCoordinator;

    private static final int SLOT_SUBMIT_LOOP_SKIP_COUNT_THRESHOLD = 10;

    /**
     * Time between successive slot submit scheduled tasks.
     * <p/>
     * In a slow message publishing scenario, this is the delay for each message for delivery.
     * For instance if we publish one message per minute then each message will have to wait
     * till this timeout before the messages are submitted to the slot coordinator.
     */
    public final int SLOT_SUBMIT_TIMEOUT;

    private SlotMessageCounter() {

        SLOT_SUBMIT_TIMEOUT = AndesConfigurationManager.readValue(
                AndesConfiguration.PERFORMANCE_TUNING_SUBMIT_SLOT_TIMEOUT);

        slotWindowSize = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_WINDOW_SIZE);

        timeOutForMessagesInQueue = AndesConfigurationManager.readValue
                (AndesConfiguration.PERFORMANCE_TUNING_SLOTS_SLOT_RETAIN_TIME_IN_MEMORY);

        slotSubmitLoopSkipCount = 0;
        slotCoordinator = MessagingEngine.getInstance().getSlotCoordinator();

        scheduleSubmitSlotToCoordinatorTimer();
    }

    /**
     * This thread is to record message IDs in slot manager when a timeout is passed
     */
    private void scheduleSubmitSlotToCoordinatorTimer() {
        submitSlotToCoordinatorTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
                Set<Map.Entry<String, Long>> slotTimeoutEntries = slotTimeOutMap.entrySet();
                for (Map.Entry<String, Long> entry : slotTimeoutEntries) {
                    if ((System.currentTimeMillis() - entry
                            .getValue()) > timeOutForMessagesInQueue) {
                        try {
                            submitSlot(entry.getKey());
                        } catch (AndesException e) {
                            // We do not do anything here since this thread will be run every 3
                            // seconds
                            log.error("Error occurred while connecting to the thrift coordinator " +
                                    e.getMessage(), e);
                        }
                    }
                }
                if (slotTimeoutEntries.isEmpty()) {
                    slotSubmitLoopSkipCount += 1;
                    if (slotSubmitLoopSkipCount == SLOT_SUBMIT_LOOP_SKIP_COUNT_THRESHOLD) {
                        //update current slot Deletion Safe Zone
                        try {
                            submitCurrentSafeZone(currentSlotDeleteSafeZone);
                            slotSubmitLoopSkipCount = 0;
                        } catch (ConnectionException e) {
                            log.error("Error while sending slot deletion safe zone update", e);
                        }
                    }
                }
            }
        }, SLOT_SUBMIT_TIMEOUT, SLOT_SUBMIT_TIMEOUT);
    }

    /**
     * Record metadata count in the current slot related to a particular queue.
     *
     * @param messageList AndesMessage list to be record
     */
    public void recordMetadataCountInSlot(Collection<AndesMessage> messageList) {
        for (AndesMessage message : messageList) {
            recordMetadataCountInSlot(message.getMetadata());
        }
    }

    /**
     * Add a new message to the count for the current slot related to a particular queue
     *
     * @param metadata AndesMessageMetadata
     */
    public void recordMetadataCountInSlot(AndesMessageMetadata metadata) {
        String storageQueueName = metadata.getStorageQueueName();
        //If this is the first message to that queue
        Slot currentSlot;
        currentSlot = updateQueueToSlotMap(metadata);
        if (currentSlot.getMessageCount() >= slotWindowSize) {
            try {
                submitSlot(storageQueueName);
            } catch (AndesException e) {
                    /*
                    We do not do anything here since this operation will be run by timeout thread also
                     */
                log.error("Error occurred while connecting to the thrift coordinator " + e
                        .getMessage(), e);
            }
        }
    }

    private void submitCurrentSafeZone(long currentSlotDeleteSafeZone) throws ConnectionException {
        slotCoordinator.updateSlotDeletionSafeZone(currentSlotDeleteSafeZone);
    }

    /**
     * Update in-memory queue to slot map. This method is is not synchronized. Single publisher should access this.
     * Ideally through a disruptor event handler
     *
     * @param metadata Andes metadata whose ID needs to be reported to SlotManager
     * @return Current slot which this metadata belongs to
     */
    private Slot updateQueueToSlotMap(AndesMessageMetadata metadata) {
        String storageQueueName = metadata.getStorageQueueName();
        Slot currentSlot = queueToSlotMap.get(storageQueueName);
        if (currentSlot == null) {
            currentSlot = new Slot();
            currentSlot.setStartMessageId(metadata.getMessageID());
            currentSlot.setEndMessageId(metadata.getMessageID());
            currentSlot.setMessageCount(1L);
            queueToSlotMap.put(storageQueueName, currentSlot);
            slotTimeOutMap.put(storageQueueName, System.currentTimeMillis());
        } else {
            long currentMsgCount = currentSlot.getMessageCount();
            long newMessageCount = currentMsgCount + 1;
            currentSlot.setMessageCount(newMessageCount);
            currentSlot.setEndMessageId(metadata.getMessageID());
            queueToSlotMap.put(storageQueueName, currentSlot);
        }
        return currentSlot;
    }

    /**
     * Submit last message ID in the slot to SlotManager.
     *
     * @param storageQueueName
     *         name of the queue which this slot belongs to
     */
    public synchronized void submitSlot(String storageQueueName) throws AndesException {
        Slot slot = queueToSlotMap.get(storageQueueName);
        if (null != slot) {
            Long slotStartTime = slotTimeOutMap.get(storageQueueName);

            // Check if the number of messages in slot is greater than or equal to slot window size or slot timeout
            // has reached. This is to avoid timer task or disruptor creating smaller/overlapping slots.
            if (slot.getMessageCount() >= slotWindowSize
                || System.currentTimeMillis() - slotStartTime >= timeOutForMessagesInQueue) {
                try {
                    slotCoordinator.updateMessageId(storageQueueName, slot.getStartMessageId(), slot.getEndMessageId());
                    queueToSlotMap.remove(storageQueueName);
                    slotTimeOutMap.remove(storageQueueName);
                } catch (ConnectionException e) {
                    // we only log here since this is called again from timer task if previous attempt failed
                    log.error("Error occurred while connecting to the thrift coordinator.", e);
                }
            }
        }
    }


    public void updateSafeZoneForNode(long currentSafeZoneVal) {
        currentSlotDeleteSafeZone = currentSafeZoneVal;
    }

    /**
     * Message id generated through {@link org.wso2.andes.kernel.disruptor.inbound.MessagePreProcessor}.
     * This Id is updated through scheduled task.
     */
    public long getCurrentNodeSafeZoneId() {
        return currentSlotDeleteSafeZone;
    }

    /**
     * @return SlotMessageCounter instance
     */
    public static SlotMessageCounter getInstance() {
        return slotMessageCounter;
    }

    /**
     * Shut down worker threads, submitSlotToCoordinatorTimer so that server can shut down properly without unexpected behaviour.
     */
    public void stop() {
        log.info("Stopping slot message counter.");
        submitSlotToCoordinatorTimer.cancel();
    }

}
