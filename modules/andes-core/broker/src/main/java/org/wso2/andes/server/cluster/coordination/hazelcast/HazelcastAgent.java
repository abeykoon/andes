/*
 * Copyright (c) 2005-2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.andes.server.cluster.coordination.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ITopic;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.kernel.AndesContext;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.slot.Slot;
import org.wso2.andes.kernel.slot.SlotState;
import org.wso2.andes.kernel.slot.SlotUtils;
import org.wso2.andes.server.cluster.HazelcastClusterAgent;
import org.wso2.andes.server.cluster.coordination.ClusterCoordinationHandler;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.server.cluster.coordination.CoordinationConstants;
import org.wso2.andes.server.cluster.coordination.SlotAgent;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.HashmapStringTreeSetWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetLongWrapper;
import org.wso2.andes.server.cluster.coordination.hazelcast.custom.serializer.wrapper.TreeSetSlotWrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


/**
 * This is a singleton class, which contains all Hazelcast related operations.
 */
public class HazelcastAgent implements SlotAgent {
    private static Log log = LogFactory.getLog(HazelcastAgent.class);

    /**
     * Value used to indicate the cluster initialization success state
     */
    private static final long INIT_SUCCESSFUL = 1L;

    /**
     * Singleton HazelcastAgent Instance.
     */
    private static HazelcastAgent hazelcastAgentInstance = new HazelcastAgent();

    /**
     * Hazelcast instance exposed by Carbon.
     */
    private HazelcastInstance hazelcastInstance;

    /**
     * Distributed topic to communicate subscription change notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> subscriptionChangedNotifierChannel;

    /**
     * Distributed topic to communicate binding change notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> bindingChangeNotifierChannel;

    /**
     * Distributed topic to communicate queue purge notifications among cluster nodes.
     */
    private ITopic<ClusterNotification> queueChangedNotifierChannel;


    /**
     * Distributed topic to communicate exchange change notification among cluster nodes.
     */
    private ITopic<ClusterNotification> exchangeChangeNotifierChannel;

    /**
     * These distributed maps are used for slot management
     */


    /**
     * distributed Map to store message ID list against queue name
     */
    private IMap<String, TreeSetLongWrapper> slotIdMap;

    /**
     * to keep track of assigned slots up to now. Key of the map contains nodeID+"_"+queueName
     */
    private IMap<String, HashmapStringTreeSetWrapper> slotAssignmentMap;

    /**
     * To keep track of slots that overlap with already assigned slots (in slotAssignmentMap). This is to ensure that
     * messages assigned to a specific assigned slot are only handled by that node itself.
     */
    private IMap<String, HashmapStringTreeSetWrapper> overLappedSlotMap;

    /**
     *distributed Map to store last assigned ID against queue name
     */
    private IMap<String, Long> lastAssignedIDMap;

    /**
     * distributed Map to store last published ID against node ID
     */
    private IMap<String, Long> lastPublishedIDMap;

    /**
     * Distributed Map to keep track of non-empty slots which are unassigned from
     * other nodes
     */
    private IMap<String, TreeSetSlotWrapper> unAssignedSlotMap;

    /**
     * This map is used to store thrift server host and thrift server port
     * map's key is port or host name.
     */
    private IMap<String,String> thriftServerDetailsMap;

    /**
     * Lock used to initialize the Slot map used by the Slot manager.
     */
    private ILock initializationLock;

    /**
     * This is used to indicate if the cluster initialization was done properly. Used a atomic long
     * since am atomic boolean is not available in the current Hazelcast implementation.
     */
    private IAtomicLong initializationDoneIndicator;

    /**
     * Hazelcast based cluster agent
      */
    private HazelcastClusterAgent clusterAgent;

    /**
     * Private constructor.
     */
    private HazelcastAgent() {

    }

    /**
     * Get singleton HazelcastAgent.
     *
     * @return HazelcastAgent
     */
    public static synchronized HazelcastAgent getInstance() {
        return hazelcastAgentInstance;
    }

    /**
     * Initialize HazelcastAgent instance.
     *
     * @param hazelcastInstance obtained hazelcastInstance from the OSGI service
     */
    @SuppressWarnings("unchecked")
    public void init(HazelcastInstance hazelcastInstance) {
        log.info("Initializing Hazelcast Agent");
        this.hazelcastInstance = hazelcastInstance;

        // Set cluster agent in Andes Context
        clusterAgent = new HazelcastClusterAgent(hazelcastInstance);
        AndesContext.getInstance().setClusterAgent(clusterAgent);

        /**
         * subscription changes
         */
        this.subscriptionChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_SUBSCRIPTION_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterSubscriptionChangedListener clusterSubscriptionChangedListener = new ClusterSubscriptionChangedListener();
        clusterSubscriptionChangedListener.addSubscriptionListener(new ClusterCoordinationHandler(this));
        this.subscriptionChangedNotifierChannel.addMessageListener(clusterSubscriptionChangedListener);


        /**
         * exchange changes
         */
        this.exchangeChangeNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_EXCHANGE_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterExchangeChangedListener clusterExchangeChangedListener = new ClusterExchangeChangedListener();
        clusterExchangeChangedListener.addExchangeListener(new ClusterCoordinationHandler(this));
        this.exchangeChangeNotifierChannel.addMessageListener(clusterExchangeChangedListener);


        /**
         * queue changes
         */
        this.queueChangedNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_QUEUE_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterQueueChangedListener clusterQueueChangedListener = new ClusterQueueChangedListener();
        clusterQueueChangedListener.addQueueListener(new ClusterCoordinationHandler(this));
        this.queueChangedNotifierChannel.addMessageListener(clusterQueueChangedListener);

        /**
         * binding changes
         */
        this.bindingChangeNotifierChannel = this.hazelcastInstance.getTopic(
                CoordinationConstants.HAZELCAST_BINDING_CHANGED_NOTIFIER_TOPIC_NAME);
        ClusterBindingChangedListener clusterBindingChangedListener = new ClusterBindingChangedListener();
        clusterBindingChangedListener.addBindingListener(new ClusterCoordinationHandler(this));
        this.bindingChangeNotifierChannel.addMessageListener(clusterBindingChangedListener);

        /**
         * Initialize hazelcast maps for slots
         */
        unAssignedSlotMap = hazelcastInstance.getMap(CoordinationConstants.UNASSIGNED_SLOT_MAP_NAME);
        slotIdMap = hazelcastInstance.getMap(CoordinationConstants.SLOT_ID_MAP_NAME);
        lastAssignedIDMap = hazelcastInstance.getMap(CoordinationConstants.LAST_ASSIGNED_ID_MAP_NAME);
        lastPublishedIDMap = hazelcastInstance.getMap(CoordinationConstants.LAST_PUBLISHED_ID_MAP_NAME);
        slotAssignmentMap = hazelcastInstance.getMap(CoordinationConstants.SLOT_ASSIGNMENT_MAP_NAME);
        overLappedSlotMap = hazelcastInstance.getMap(CoordinationConstants.OVERLAPPED_SLOT_MAP_NAME);

        /**
         * Initialize hazelcast map fot thrift server details
         */
        thriftServerDetailsMap = hazelcastInstance.getMap(CoordinationConstants.THRIFT_SERVER_DETAILS_MAP_NAME);

        /**
         * Initialize distributed lock and boolean related to slot map initialization
         */
        initializationLock = hazelcastInstance.getLock(CoordinationConstants.INITIALIZATION_LOCK);
        initializationDoneIndicator = hazelcastInstance
                .getAtomicLong(CoordinationConstants.INITIALIZATION_DONE_INDICATOR);

        log.info("Successfully initialized Hazelcast Agent");
    }

    public void notifySubscriptionsChanged(ClusterNotification clusterNotification) throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        }
        try {
            this.subscriptionChangedNotifierChannel.publish(clusterNotification);
        } catch (Exception ex) {
            log.error("Error while sending subscription change notification : " + clusterNotification.getEncodedObjectAsString(), ex);
            throw new AndesException("Error while sending queue change notification : " + clusterNotification.getEncodedObjectAsString(), ex);
        }

    }

    public void notifyQueuesChanged(ClusterNotification clusterNotification) throws AndesException {

        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        }
        try {
            this.queueChangedNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending queue change notification : " + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending queue change notification : " + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    public void notifyExchangesChanged(ClusterNotification clusterNotification) throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("Sending GOSSIP: " + clusterNotification.getDescription());
        }
        try {
            this.exchangeChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending exchange change notification" + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending exchange change notification" + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    public void notifyBindingsChanged(ClusterNotification clusterNotification) throws AndesException {
        if (log.isDebugEnabled()) {
            log.debug("GOSSIP: " + clusterNotification.getDescription());
        }
        try {
            this.bindingChangeNotifierChannel.publish(clusterNotification);
        } catch (Exception e) {
            log.error("Error while sending binding change notification" + clusterNotification.getEncodedObjectAsString(), e);
            throw new AndesException("Error while sending binding change notification" + clusterNotification.getEncodedObjectAsString(), e);
        }
    }

    /**
     * This method returns a map containing thrift server port and hostname
     * @return thriftServerDetailsMap
     */
    public IMap<String, String> getThriftServerDetailsMap() {
        return thriftServerDetailsMap;
    }

    /**
     * Acquire the distributed lock related to cluster initialization. This lock is required to
     * avoid two nodes initializing the map twice.
     */
    public void acquireInitializationLock() {
        if (log.isDebugEnabled()) {
            log.debug("Trying to acquire initialization lock.");
        }

        initializationLock.lock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock acquired.");
        }
    }

    /**
     * Inform other members in the cluster that the cluster was initialized properly.
     */
    public void indicateSuccessfulInitilization() {
        initializationDoneIndicator.set(INIT_SUCCESSFUL);
    }

    /**
     * Check if a member has already initialized the cluster
     *
     * @return true if cluster is already initialized
     */
    public boolean isClusterInitializedSuccessfully() {
        return initializationDoneIndicator.get() == INIT_SUCCESSFUL;
    }

    /**
     * Release the initialization lock.
     */
    public void releaseInitializationLock() {
        initializationLock.unlock();

        if (log.isDebugEnabled()) {
            log.debug("Initialization lock released.");
        }
    }

    /**
     * Method to check if the hazelcast instance has shutdown.
     * @return boolean
     */
    public boolean isActive() {
        if (null != hazelcastInstance) {
            return hazelcastInstance.getLifecycleService().isRunning();
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createSlot(long startMessageId, long endMessageId, String storageQueueName, String assignedNodeId)
            throws AndesException {
        //createSlot() method in Hazelcast agent does not need to perform anything
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlot(String nodeId, String queueName, long startMessageId, long endMessageId) throws AndesException {
        try {
            HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
            HashmapStringTreeSetWrapper wrapper = this.slotAssignmentMap.get(nodeId);
            if (null != wrapper) {
                queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                TreeSet<Slot> currentSlotList = queueToSlotMap.get(queueName);
                if (currentSlotList != null) {
                    // com.google.gson.Gson gson = new GsonBuilder().create();
                    //get the actual reference of the slot to be removed
                    Slot slotInAssignmentMap = null; //currentSlotList.ceiling(emptySlot);
                    for (Slot slot : currentSlotList) {
                        if (slot.getStartMessageId() == startMessageId) {
                            slotInAssignmentMap = slot;
                        }
                    }
                    if (null != slotInAssignmentMap) {
                        if (slotInAssignmentMap.addState(SlotState.DELETED)) {
                            currentSlotList.remove(slotInAssignmentMap);
                            queueToSlotMap.put(queueName, currentSlotList);
                            wrapper.setStringListHashMap(queueToSlotMap);
                            slotAssignmentMap.set(nodeId, wrapper);
                        }
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to delete slot for queue : " +
                    queueName + " from node " + nodeId, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotAssignmentByQueueName(String nodeId, String queueName) throws AndesException {
        try {
            TreeSet<Slot> slotListToReturn = new TreeSet<>();
            //Get assigned slots from Hazelcast, delete all belonging to queue
            //and set back
            HashmapStringTreeSetWrapper wrapper = this.slotAssignmentMap.get(nodeId);
            HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
            if (null != wrapper) {
                queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                TreeSet<Slot> assignedSlotList = queueToSlotMap.remove(queueName);
                if(assignedSlotList != null) {
                    slotListToReturn.addAll(assignedSlotList);
                }
                wrapper.setStringListHashMap(queueToSlotMap);
                this.slotAssignmentMap.set(nodeId, wrapper);
            }

            //Get overlapped slots from Hazelcast, delete all belonging to queue and
            //set back
            HashmapStringTreeSetWrapper overlappedSlotWrapper = this.overLappedSlotMap.get(nodeId);
            HashMap<String, TreeSet<Slot>> queueToOverlappedSlotMap = null;
            if (null != overlappedSlotWrapper) {
                queueToOverlappedSlotMap = overlappedSlotWrapper.getStringListHashMap();
            }
            if (queueToOverlappedSlotMap != null) {
                TreeSet<Slot> assignedOverlappedSlotList = queueToOverlappedSlotMap.remove(queueName);
                if(assignedOverlappedSlotList != null) {
                    slotListToReturn.addAll(assignedOverlappedSlotList);
                }
                overlappedSlotWrapper.setStringListHashMap(queueToOverlappedSlotMap);
                this.overLappedSlotMap.set(nodeId, overlappedSlotWrapper);
            }

            //add the deleted slots to un-assigned slot map, so that they can be assigned again.
            if (!slotListToReturn.isEmpty()) {
                TreeSetSlotWrapper treeSetStringWrapper = unAssignedSlotMap.get(queueName);
                TreeSet<Slot> unAssignedSlotSet = new TreeSet<>();
                if (null != treeSetStringWrapper) {
                    unAssignedSlotSet = treeSetStringWrapper.getSlotTreeSet();
                } else {
                    treeSetStringWrapper = new TreeSetSlotWrapper();
                }
                if (unAssignedSlotSet == null) {
                    unAssignedSlotSet = new TreeSet<>();
                }
                for (Slot returnSlot : slotListToReturn) {
                    //Reassign only if the slot is not empty
                    if (!SlotUtils.checkSlotEmptyFromMessageStore(returnSlot)) {
                        if (returnSlot.addState(SlotState.RETURNED)) {
                            unAssignedSlotSet.add(returnSlot);
                        }
                    }
                    treeSetStringWrapper.setSlotTreeSet(unAssignedSlotSet);
                    unAssignedSlotMap.set(queueName, treeSetStringWrapper);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to delete slot assignment for queue : " +
                    queueName + " from node " + nodeId, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getUnAssignedSlot(String queueName) throws AndesException {
        Slot slotToBeAssigned = null;
        try {
            TreeSetSlotWrapper unAssignedSlotWrapper = unAssignedSlotMap.get(queueName);
            if (null != unAssignedSlotWrapper) {
                TreeSet<Slot> slotsFromUnassignedSlotMap = unAssignedSlotWrapper.getSlotTreeSet();
                if (slotsFromUnassignedSlotMap != null && !slotsFromUnassignedSlotMap.isEmpty()) {
                    //Get and remove slot and update hazelcast map
                    slotToBeAssigned = slotsFromUnassignedSlotMap.pollFirst();
                    unAssignedSlotWrapper.setSlotTreeSet(slotsFromUnassignedSlotMap);
                    unAssignedSlotMap.set(queueName, unAssignedSlotWrapper);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to get unassigned slot for queue : " +
                    queueName, ex);
        }
        return slotToBeAssigned;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateSlotAssignment(String nodeId, String queueName, Slot allocatedSlot) throws AndesException {
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;

        try {
            HashmapStringTreeSetWrapper wrapper = this.slotAssignmentMap.get(nodeId);
            if (wrapper == null) {
                wrapper = new HashmapStringTreeSetWrapper();
                queueToSlotMap = new HashMap<>();
                wrapper.setStringListHashMap(queueToSlotMap);
                this.slotAssignmentMap.putIfAbsent(nodeId, wrapper);
            }
            wrapper = this.slotAssignmentMap.get(nodeId);
            queueToSlotMap = wrapper.getStringListHashMap();
            currentSlotList = queueToSlotMap.get(queueName);
            if (currentSlotList == null) {
                currentSlotList = new TreeSet<>();
            }

            //update slot state
            if (allocatedSlot.addState(SlotState.ASSIGNED)) {
                //remove any similar slot from hazelcast and add the updated one
                currentSlotList.remove(allocatedSlot);
                currentSlotList.add(allocatedSlot);
                queueToSlotMap.put(queueName, currentSlotList);
                wrapper.setStringListHashMap(queueToSlotMap);
                this.slotAssignmentMap.set(nodeId, wrapper);
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to update slot assignment for queue : " +
                    queueName + " from node " + nodeId, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getQueueToLastAssignedId(String queueName) throws AndesException {
        long lastAssignedId = this.lastAssignedIDMap.get(queueName) != null ?
                this.lastAssignedIDMap.get(queueName) : 0L;
        return lastAssignedId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueueToLastAssignedId(String queueName, long lastAssignedId) throws AndesException {
        this.lastAssignedIDMap.set(queueName, lastAssignedId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getNodeToLastPublishedId(String nodeId) throws AndesException {
        return this.lastPublishedIDMap.get(nodeId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setNodeToLastPublishedId(String nodeId, long lastPublishedId) throws AndesException {
        this.lastPublishedIDMap.set(nodeId, lastPublishedId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePublisherNode(String nodeId) throws AndesException {
        lastPublishedIDMap.delete(nodeId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<String> getMessagePublishedNodes() throws AndesException {
        TreeSet<String> messagePublishedNodes = new TreeSet<>();
        messagePublishedNodes.addAll(this.lastPublishedIDMap.keySet());
        return messagePublishedNodes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSlotState(long startMessageId, long endMessageId, SlotState slotState) throws AndesException {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Slot getOverlappedSlot(String nodeId, String queueName) throws AndesException {
        Slot slotToBeAssigned = null;
        TreeSet<Slot> currentSlotList;
        HashMap<String, TreeSet<Slot>> queueToSlotMap;
        try {
            HashmapStringTreeSetWrapper wrapper = this.overLappedSlotMap.get(nodeId);
            if (null != wrapper) {
                queueToSlotMap = wrapper.getStringListHashMap();
                currentSlotList = queueToSlotMap.get(queueName);
                if (null != currentSlotList && !currentSlotList.isEmpty()) {
                    //get and remove slot
                    slotToBeAssigned = currentSlotList.pollFirst();
                    queueToSlotMap.put(queueName, currentSlotList);
                    //update hazelcast map
                    wrapper.setStringListHashMap(queueToSlotMap);
                    this.overLappedSlotMap.set(nodeId, wrapper);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to getOverlappedSlot for queue : " +
                    queueName + " from node " + nodeId, ex);
        }
        return slotToBeAssigned;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageId(String queueName, long messageId) throws AndesException {
        try {
            TreeSet<Long> messageIdSet = this.getMessageIds(queueName);
            TreeSetLongWrapper wrapper = this.slotIdMap.get(queueName);
            messageIdSet.add(messageId);
            wrapper.setLongTreeSet(messageIdSet);
            this.slotIdMap.set(queueName, wrapper);
        }  catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to addMessageId for queue : " +
                    queueName, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException {
        TreeSetLongWrapper wrapper = null;
        try {
            wrapper = this.slotIdMap.get(queueName);
            if (wrapper == null) {
                wrapper = new TreeSetLongWrapper();
                this.slotIdMap.putIfAbsent(queueName, wrapper);
            }
        }  catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to getMessageIds for queue : " +
                    queueName, ex);
        }
        return wrapper.getLongTreeSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageId(String queueName, long messageId) throws AndesException {
        try {
            TreeSetLongWrapper wrapper = this.slotIdMap.get(queueName);
            TreeSet<Long> messageIDSet;
            messageIDSet = wrapper.getLongTreeSet();
            if (messageIDSet != null && !messageIDSet.isEmpty()) {
                messageIDSet.pollFirst();
                //set modified published ID map to hazelcast
                wrapper.setLongTreeSet(messageIDSet);
                this.slotIdMap.set(queueName, wrapper);
            }
        }  catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to deleteMessageId for queue : " +
                    queueName, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteSlotsByQueueName(String queueName) throws AndesException {
        try {
            if (null != this.unAssignedSlotMap) {
                this.unAssignedSlotMap.remove(queueName);
            }

            // The requirement here is to clear slot associations for the queue on all nodes.
            List<String> nodeIDs = AndesContext.getInstance().getClusterAgent().getAllNodeIdentifiers();

            for (String nodeID : nodeIDs) {
                HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeID);
                HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
                if (null != wrapper) {
                    queueToSlotMap = wrapper.getStringListHashMap();
                }
                if (queueToSlotMap != null) {
                    queueToSlotMap.remove(queueName);
                    wrapper.setStringListHashMap(queueToSlotMap);
                    slotAssignmentMap.set(nodeID, wrapper);
                }

                //clear overlapped slot map
                HashmapStringTreeSetWrapper overlappedSlotsWrapper = overLappedSlotMap.get(nodeID);
                if (null != overlappedSlotsWrapper) {
                    HashMap<String, TreeSet<Slot>> queueToOverlappedSlotMap = null;
                    if (null != wrapper) {
                        queueToOverlappedSlotMap = overlappedSlotsWrapper.getStringListHashMap();
                    }
                    if (queueToSlotMap != null) {
                        queueToOverlappedSlotMap.remove(queueName);
                        overlappedSlotsWrapper.setStringListHashMap(queueToOverlappedSlotMap);
                        overLappedSlotMap.set(nodeID, overlappedSlotsWrapper);
                    }
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to deleteSlotsByQueueName for queue : " +
                    queueName, ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
        if (null != this.slotIdMap) {
            this.slotIdMap.remove(queueName);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAssignedSlotsByNodeId(String nodeId) throws AndesException {
        TreeSet<Slot> resultSet = new TreeSet<>();
        try {
            HashmapStringTreeSetWrapper wrapper = this.slotAssignmentMap.remove(nodeId);
            HashMap<String, TreeSet<Slot>> queueToSlotMap = null;
            if (null != wrapper) {
                queueToSlotMap = wrapper.getStringListHashMap();
            }
            if (queueToSlotMap != null) {
                for (Map.Entry<String, TreeSet<Slot>> entry : queueToSlotMap.entrySet()) {
                    TreeSet<Slot> slotsToBeReAssigned = entry.getValue();
                    resultSet.addAll(slotsToBeReAssigned);
                }
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to deleteSlotsByQueueName for node : " +
                    nodeId, ex);
        }
        return resultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TreeSet<Slot> getAllSlotsByQueueName(String nodeId, String queueName) throws AndesException {
        // Sweep all assigned slots to find overlaps using slotAssignmentMap,
        // cos its optimized for node,queue-wise iteration.
        // The requirement here is to clear slot associations for the queue on all nodes.

        TreeSet<Slot> resultSet = new TreeSet<>();
        HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
        if (!overLappedSlotMap.containsKey(nodeId)) {
            overLappedSlotMap.put(nodeId, new HashmapStringTreeSetWrapper());
        }
        HashmapStringTreeSetWrapper olWrapper = overLappedSlotMap.get(nodeId);
        HashMap<String, TreeSet<Slot>> olSlotMap = olWrapper.getStringListHashMap();
        if (!olSlotMap.containsKey(queueName)) {
            olSlotMap.put(queueName, new TreeSet<Slot>());
            olWrapper.setStringListHashMap(olSlotMap);
            overLappedSlotMap.set(nodeId, olWrapper);
        }
        if (null != wrapper) {
            HashMap<String, TreeSet<Slot>> queueToSlotMap = wrapper.getStringListHashMap();
            if (queueToSlotMap != null) {
                TreeSet<Slot> slotListForQueueOnNode = queueToSlotMap.get(queueName);
                if(null != slotListForQueueOnNode ) {
                    resultSet.addAll(slotListForQueueOnNode);
                }
            }
        }
        return resultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reAssignSlot(Slot slotToBeReAssigned) throws AndesException {
        try {
            TreeSet<Slot> freeSlotTreeSet = new TreeSet<>();
            TreeSetSlotWrapper treeSetStringWrapper = new TreeSetSlotWrapper();

            treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);

            this.unAssignedSlotMap.putIfAbsent(slotToBeReAssigned.getStorageQueueName(),
                    treeSetStringWrapper);

            if (slotToBeReAssigned.addState(SlotState.RETURNED)) {
                treeSetStringWrapper = this.unAssignedSlotMap.get(slotToBeReAssigned.getStorageQueueName());
                freeSlotTreeSet = treeSetStringWrapper.getSlotTreeSet();
                freeSlotTreeSet.add(slotToBeReAssigned);
                treeSetStringWrapper.setSlotTreeSet(freeSlotTreeSet);
                this.unAssignedSlotMap.set(slotToBeReAssigned.getStorageQueueName(), treeSetStringWrapper);
            }
        } catch (HazelcastInstanceNotActiveException ex) {
            throw new AndesException("Failed to reAssignSlot", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteOverlappedSlots(String nodeId) throws AndesException {
        this.overLappedSlotMap.remove(nodeId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOverlappedSlots(String nodeId, String queueName, TreeSet<Slot> overlappedSlots) throws AndesException {
        HashmapStringTreeSetWrapper wrapper = slotAssignmentMap.get(nodeId);
        HashMap<String, TreeSet<Slot>> queueToSlotMap = new HashMap<>();
        if(null == wrapper) {
            wrapper = new HashmapStringTreeSetWrapper();
        } else {
            queueToSlotMap = wrapper.getStringListHashMap();
        }
        HashmapStringTreeSetWrapper olWrapper = overLappedSlotMap.get(nodeId);
        HashMap<String, TreeSet<Slot>> olSlotMap = olWrapper.getStringListHashMap();
        for(Slot slot : overlappedSlots) {
            //Add to global overlappedSlotMap
            olSlotMap.get(queueName).remove(slot);
            olSlotMap.get(queueName).add(slot);
        }
        wrapper.setStringListHashMap(queueToSlotMap);
        slotAssignmentMap.set(nodeId, wrapper);
        // Add all marked slots collected into the olSlot to global overlappedSlotsMap.
        olWrapper.setStringListHashMap(olSlotMap);
        overLappedSlotMap.set(nodeId, olWrapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueues() throws AndesException{
        return this.slotIdMap.keySet();
    }
}
