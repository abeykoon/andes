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
import org.wso2.andes.kernel.AndesException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This Runnable will calculate safe zone for the cluster time to time.
 * In normal cases, this should run as long as the Slot manager node is alive
 */
public class SlotDeleteSafeZoneCalc implements Runnable {

    private static Log log = LogFactory.getLog(SlotDeleteSafeZoneCalc.class);

    private AtomicLong slotDeleteSafeZone;

    private boolean running;

    private boolean isLive;

    private int seekInterval;


    /**
     * Define a safe zone calculator. This will run once seekInterval
     * When created by default it is marked as live
     *
     * @param seekInterval interval in milliseconds calculation should run
     */
    public SlotDeleteSafeZoneCalc(int seekInterval) {
        this.seekInterval = seekInterval;
        this.running = true;
        this.isLive = true;
        this.slotDeleteSafeZone = new AtomicLong(Long.MAX_VALUE);
        
    }

    @Override
    public void run() {
        if (log.isDebugEnabled()) {
            log.debug("Slot deletion safe zone calculation started.");
        }
        while (running) {
            if (isLive) {
                Set<String> nodesWithPublishedMessages;
                try {
                    nodesWithPublishedMessages = SlotManagerClusterMode.getInstance().getMessagePublishedNodes();
                } catch (AndesException e) {
                    log.error("SlotDeleteSafeZoneCalc stopped due to failing to get message published nodes.");
                    this.setLive(false);
                    continue;
                }
                Map<String, Long> nodeInformedSafeZones =
                        SlotManagerClusterMode.getInstance().getNodeInformedSlotDeletionSafeZones();

                /** calculate safe zone (minimum value of messageIDs published so far to the
                 * cluster by each node)
                 */
                long globalSafeZoneVal = Long.MAX_VALUE;
                for (String nodeID : nodesWithPublishedMessages) {

                    long safeZoneValue = Long.MAX_VALUE;

                    //get the maximum message id published by node so far
                    Long safeZoneByPublishedMessages = null;
                    try {
                        safeZoneByPublishedMessages = SlotManagerClusterMode.getInstance()
                                .getLastPublishedIDByNode(nodeID);
                    } catch (AndesException e) {
                        log.error("SlotDeleteSafeZoneCalc stopped due to failing to get last published id for node:" +
                                nodeID);
                        this.setLive(false);
                        continue;
                    }

                    if (null != safeZoneByPublishedMessages) {
                        safeZoneValue = safeZoneByPublishedMessages;
                    }

                    //If messages are not published, each node will send a messageID giving
                    // assurance that next message id it would generate will be beyond a certain
                    // number
                    Long nodeInformedSafeZone = nodeInformedSafeZones.get(nodeID);

                    /**
                     * if no new messages are published and no new slot assignment happened
                     * node informed value can be bigger. We need to accept that to keep the
                     * safe zone moving
                     */
                    if (null != nodeInformedSafeZone) {
                        if (Long.MAX_VALUE != safeZoneValue) {
                            if (safeZoneValue < nodeInformedSafeZone) {
                                safeZoneValue = nodeInformedSafeZone;
                            }
                        } else {
                            safeZoneValue = nodeInformedSafeZone;
                        }
                    }

                    if (globalSafeZoneVal > safeZoneValue) {
                        globalSafeZoneVal = safeZoneValue;
                    }
                }

                slotDeleteSafeZone.set(globalSafeZoneVal);

                if (log.isDebugEnabled()) {
                    log.debug("Safe Zone Calculated : " + slotDeleteSafeZone);
                }

                try {
                    Thread.sleep(seekInterval);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            } else {
                try {
                    Thread.sleep(15 * 1000);
                } catch (InterruptedException e) {
                    //silently ignore
                }
            }
        }
    }

    /**
     * Get slot deletion safe zone calculated in last iteration
     *
     * @return current clot deletion safe zone
     */
    public long getSlotDeleteSafeZone() {
        return slotDeleteSafeZone.get();
    }

    /**
     * Specifically set slot deletion safe zone
     *
     * @param slotDeleteSafeZone safe zone value to be set
     */
    public void setSlotDeleteSafeZone(long slotDeleteSafeZone) {
        this.slotDeleteSafeZone.set(slotDeleteSafeZone);
    }

    /**
     * Check if safe zone calculator is running
     *
     * @return true if calc is running
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Define if the calc thread should run. When staring the thread
     * this should be set to true. Setting false will destroy the calc
     * thread.
     *
     * @param running if the calc thread should run
     */
    public void setRunning(boolean running) {
        this.running = running;
    }

    /**
     * Check if calc thread is doing calculations actively.
     *
     * @return if calc is doing calculations.
     */
    public boolean isLive() {
        return isLive;
    }

    /**
     * Define if calc thread should do calculations. Setting to false
     * will not destroy thread but will stop calculations.
     *
     * @param isLive set if calc should do calculations
     */
    public void setLive(boolean isLive) {
        this.isLive = isLive;
    }

    /**
     * Set the interval calc thread is running
     *
     * @return seek interval
     */
    public int getSeekInterval() {
        return seekInterval;
    }

    /**
     * Set interval calc thread should run. Calculating safe zone will be done
     * once this interval. Set in milliseconds.
     *
     * @param seekInterval interval in milliseconds.
     */
    public void setSeekInterval(int seekInterval) {
        this.seekInterval = seekInterval;
    }
}
