/*
* Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
*
* WSO2 Inc. licenses this file to you under the Apache License,
* Version 2.0 (the "License"); you may not use this file except
* in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied. See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.wso2.andes.kernel;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.andes.server.queue.QueueEntry;

/**
 * This class represents message expiration Delivery Rule
 */
public class MessageExpiredRule implements DeliveryRule {
    private static Log log = LogFactory.getLog(MessageExpiredRule.class);

    /**
     * Used to get message information
     */
    private OnflightMessageTracker onflightMessageTracker;

    public MessageExpiredRule() {
        onflightMessageTracker = OnflightMessageTracker.getInstance();
    }

    /**
     * Evaluating the message expiration delivery rule
     *
     * @return isOKToDelivery
     */
    @Override
    public boolean evaluate(QueueEntry message) {
        long messageID = message.getMessage().getMessageNumber();
        //Check if destination entry has expired. Any expired message will not be delivered
        if (onflightMessageTracker.isMsgExpired(messageID)) {
            log.warn("Message is expired. Routing Message to DLC : id= " + messageID);
            return false;
        } else {
            return true;
        }
    }
}
