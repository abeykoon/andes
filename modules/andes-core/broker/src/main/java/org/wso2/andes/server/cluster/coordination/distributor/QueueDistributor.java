/*
 * Copyright (c) 2017, WSO2 Inc. (http://wso2.com) All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wso2.andes.server.cluster.coordination.distributor;


import org.wso2.andes.configuration.AndesConfigurationManager;
import org.wso2.andes.configuration.enums.AndesConfiguration;
import org.wso2.andes.kernel.AndesContextStore;
import org.wso2.andes.kernel.AndesException;

public class QueueDistributor {

    private AndesContextStore contextStore;
    private QueueDistributeStrategy queueDistributeStategy;

    public QueueDistributor(AndesContextStore contextStore) {
        this.contextStore = contextStore;
        if ("AllToOne".equals(AndesConfigurationManager.readValue(AndesConfiguration.HA_QUEUE_DISTRIBUTION_STRATEGY))) {
            this.queueDistributeStategy = new AllToOneQueueDistributor(contextStore);
        } else {
            this.queueDistributeStategy = new RoundRobinQueueDistributor(contextStore);
        }
    }

    public synchronized NodeInfo getMasterNode(String queueName, String protocol) throws AndesException {
        NodeInfo nodeToAssignQueue;
        //check if queue is already assigned
        nodeToAssignQueue = contextStore.getQueueOwningNode(queueName);
        //if not assigned decide to which node
        if(null == nodeToAssignQueue) {
            nodeToAssignQueue = queueDistributeStategy.getMasterNode(queueName);
            //store assignment in persistent store
            contextStore.assignNodeForQueue(queueName, nodeToAssignQueue.getNodeID());
        }
        return nodeToAssignQueue;
    }
}
