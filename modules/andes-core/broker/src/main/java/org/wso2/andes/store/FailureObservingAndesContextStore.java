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

package org.wso2.andes.store;

import org.apache.log4j.Logger;
import org.wso2.andes.configuration.util.ConfigurationProperties;
import org.wso2.andes.kernel.AndesBinding;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.DurableStoreConnection;
import org.wso2.andes.kernel.router.AndesMessageRouter;
import org.wso2.andes.server.cluster.coordination.rdbms.MembershipEvent;
import org.wso2.andes.server.cluster.NodeHeartBeatData;
import org.wso2.andes.server.cluster.coordination.ClusterNotification;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.StorageQueue;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Implementation of {@link AndesContextStore} which observes failures such is
 * connection errors. Any {@link AndesContextStore} implementation specified in
 * broker.xml will be wrapped by this class.
 *
 */
public class FailureObservingAndesContextStore implements AndesContextStore {

    /**
     * {@link AndesContextStore} specified in broker.xml
     */
    private AndesContextStore wrappedAndesContextStoreInstance;

    private static final Logger log = Logger.getLogger(FailureObservingAndesContextStore.class);

    /**
     * Variable that holds the operational status of the context store. True if store is operational.
     */
    private AtomicBoolean isStoreAvailable;

    /**
     * Future referring to a scheduled task which check the connectivity to the
     * store.
     * Used to cancel the periodic task after store becomes operational.
     */
    private ScheduledFuture<?> storeHealthDetectingFuture;

    /**
     * {@link FailureObservingStoreManager} to notify about store's operational status.
     */
    private FailureObservingStoreManager failureObservingStoreManager;

    /**
     * {@inheritDoc}
     */
    public FailureObservingAndesContextStore(AndesContextStore wrapped, FailureObservingStoreManager manager) {
        this.wrappedAndesContextStoreInstance = wrapped;
        isStoreAvailable = new AtomicBoolean(true);
        storeHealthDetectingFuture = null;
        failureObservingStoreManager = manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DurableStoreConnection init(ConfigurationProperties connectionProperties) throws AndesException {

        try {
            return wrappedAndesContextStoreInstance.init(connectionProperties);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, List<String>> getAllStoredDurableSubscriptions() throws AndesException {

        try {
            return wrappedAndesContextStoreInstance.getAllStoredDurableSubscriptions();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllDurableSubscriptionsByID() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllDurableSubscriptionsByID();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSubscriptionExist(String subscriptionId) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.isSubscriptionExist(subscriptionId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int updateDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.updateDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateOrInsertDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.updateOrInsertDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateDurableSubscriptions(Map<String, String> subscriptions) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.updateDurableSubscriptions(subscriptions);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeDurableSubscription(AndesSubscription subscription) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeDurableSubscription(subscription);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNodeDetails(String nodeID, String data) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeNodeDetails(nodeID, data);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void assignNodeForQueue(String queueName, String nodeID) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.assignNodeForQueue(queueName, nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     * @param queueName
     */
    @Override
    public String getQueueOwningNode(String queueName) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getQueueOwningNode(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     * @param queueName
     */
    @Override
    public void updateQueueOwningNode(String queueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.updateQueueOwningNode(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    @Override
    public void removeQueueOwningInformation(String queueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeQueueOwningInformation(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    @Override
    public List<String> getAllQueuesOwnedByNode(String nodeID) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllQueuesOwnedByNode(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getAllStoredNodeData() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllStoredNodeData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeData(String nodeID) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeNodeData(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.addMessageCounterForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getMessageCountForQueue(String destinationQueueName) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getMessageCountForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetMessageCounterForQueue(String storageQueueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.resetMessageCounterForQueue(storageQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeMessageCounterForQueue(String destinationQueueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeMessageCounterForQueue(destinationQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void incrementMessageCountForQueue(String destinationQueueName, long incrementBy) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.incrementMessageCountForQueue(destinationQueueName, incrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void decrementMessageCountForQueue(String destinationQueueName, long decrementBy) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.decrementMessageCountForQueue(destinationQueueName, decrementBy);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeExchangeInformation(String exchangeName, String exchangeInfo) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeExchangeInformation(exchangeName, exchangeInfo);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesMessageRouter> getAllMessageRoutersStored() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllMessageRoutersStored();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteExchangeInformation(String exchangeName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.deleteExchangeInformation(exchangeName);

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeQueueInformation(String queueName, String queueInfo) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeQueueInformation(queueName, queueInfo);

        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<StorageQueue> getAllQueuesStored() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllQueuesStored();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteQueueInformation(String queueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.deleteQueueInformation(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeBindingInformation(String exchange, String boundQueueName, String bindingInfo)
            throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeBindingInformation(exchange, boundQueueName, bindingInfo);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<AndesBinding> getBindingsStoredForExchange(String exchangeName) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getBindingsStoredForExchange(exchangeName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteBindingInformation(String exchangeName, String boundQueueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.deleteBindingInformation(exchangeName, boundQueueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        wrappedAndesContextStoreInstance.close();
    }

    /**
     * {@inheritDoc}.
     * <p>
     * Alters the behavior where
     * <ol>
     * <li>checks the operational status of the wrapped context store</li>
     * <li>if context store is operational it will cancel the periodic task</li>
     * </ol>
     */
    @Override
    public boolean isOperational(String testString, long testTime) {
        if (wrappedAndesContextStoreInstance.isOperational(testString, testTime)) {
            isStoreAvailable.set(true);
            if (storeHealthDetectingFuture != null) {
                // we have detected that store is operational therefore
                // we don't need to run the periodic task to check weather store
                // is available.
                storeHealthDetectingFuture.cancel(false);
                storeHealthDetectingFuture = null;
            }
            failureObservingStoreManager.notifyContextStoreOperational(this);
            return true;
        }
        return false;
    }

    /**
     * A convenient method to notify all {@link StoreHealthListener}s that
     * context store became offline
     *
     * @param e
     *            the exception occurred.
     */
    private void notifyFailures(AndesStoreUnavailableException e) {

        if (isStoreAvailable.compareAndSet(true, false)) {
            log.warn("Context store became non-operational");
            failureObservingStoreManager.notifyContextStoreNonOperational(e, wrappedAndesContextStoreInstance);
            storeHealthDetectingFuture = failureObservingStoreManager.scheduleHealthCheckTask(this);
        }
    }

    /**
     * Delete message ids by queue name
     *
     * @param queueName name of queue
     * @throws AndesException
     */
    @Override
    public void deleteMessageIdsByQueueName(String queueName) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.deleteMessageIdsByQueueName(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removePublisherNodeId(String nodeId) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removePublisherNodeId(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get all message published nodes
     *
     * @return set of published nodes
     * @throws AndesException
     */
    @Override
    public TreeSet<String> getMessagePublishedNodes() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getMessagePublishedNodes();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Add message ids to store
     *
     * @param queueName name of queue
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void addMessageId(String queueName, long messageId) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.addMessageId(queueName, messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Get message ids for a given queue
     *
     * @param queueName name of queue
     * @return set of message ids
     * @throws AndesException
     */
    @Override
    public TreeSet<Long> getMessageIds(String queueName) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getMessageIds(queueName);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * Delete a message id
     *
     * @param messageId id of message
     * @throws AndesException
     */
    @Override
    public void deleteMessageId(long messageId) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.deleteMessageId(messageId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueues() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllQueues();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getAllQueuesInSubmittedSlots() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllQueuesInSubmittedSlots();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }


    /**
     * Clear and reset slot storage
     *
     * @throws AndesException
     */
    @Override
    public void clearSlotStorage() throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearSlotStorage();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean createCoordinatorEntry(String nodeId, InetSocketAddress thriftAddress) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.createCoordinatorEntry(nodeId, thriftAddress);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIsCoordinator(String nodeId) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.checkIsCoordinator(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateCoordinatorHeartbeat(String nodeId) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.updateCoordinatorHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean checkIfCoordinatorValid(int age) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.checkIfCoordinatorValid(age);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getCoordinatorThriftAddress() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getCoordinatorThriftAddress();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeCoordinator() throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeCoordinator();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateNodeHeartbeat(String nodeId) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.updateNodeHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createNodeHeartbeatEntry(String nodeId,  InetSocketAddress nodeAddress) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.createNodeHeartbeatEntry(nodeId, nodeAddress);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeHeartBeatData> getAllHeartBeatData() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getAllHeartBeatData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void removeNodeHeartbeat(String nodeId) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.removeNodeHeartbeat(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markNodeAsNotNew(String nodeId) throws AndesException{
        try {
            wrappedAndesContextStoreInstance.markNodeAsNotNew(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCoordinatorNodeId() throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.getCoordinatorNodeId();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    @Override
    public void clearHeartBeatData() throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearHeartBeatData();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeMembershipEvent(List<String> clusterNodes, int membershipEventType, String changedMember)
            throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeMembershipEvent(clusterNodes, membershipEventType, changedMember);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<MembershipEvent> readMemberShipEvents(String nodeID) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.readMemberShipEvents(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearMembershipEvents() throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearMembershipEvents();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearMembershipEvents(String nodeID) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearMembershipEvents(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void storeClusterNotification(List<String> clusterNodes, String originatedNode, String artifactType, String
            clusterNotificationType, String notification, String description) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.storeClusterNotification(clusterNodes, originatedNode,
                    artifactType, clusterNotificationType, notification, description);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}.
     */
    public List<ClusterNotification> readClusterNotifications(String nodeID) throws AndesException {
        try {
            return wrappedAndesContextStoreInstance.readClusterNotifications(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearClusterNotifications() throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearClusterNotifications();
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearClusterNotifications(String nodeID) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearClusterNotifications(nodeID);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }

    /**
     * {@inheritDoc}
     */
    public void clearQueueAssignementsForNode(String nodeId) throws AndesException {
        try {
            wrappedAndesContextStoreInstance.clearQueueAssignementsForNode(nodeId);
        } catch (AndesStoreUnavailableException exception) {
            notifyFailures(exception);
            throw exception;
        }
    }
}
