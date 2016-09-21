/*
 * Copyright (c) 2016, WSO2 Inc. (http://wso2.com) All Rights Reserved.
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

package org.wso2.andes.kernel.disruptor.inbound;

import org.wso2.andes.kernel.AndesException;
import org.wso2.andes.kernel.subscription.AndesSubscription;
import org.wso2.andes.kernel.subscription.AndesSubscriptionManager;

import java.util.UUID;

/**
 * Inbound event representation of a recover messages (recover sent but not acked messages)
 */
public class InboundMessageRecoverEvent implements AndesInboundStateEvent {

    private static final String EVENT_TYPE = "MESSAGE_RECOVER";

    private UUID channelID;

    /**
     * Reference to AndesSubscriptionManager to get subscription by channel
     */
    private AndesSubscriptionManager andesSubscriptionManager;

    /**
     * Create an inbound event to recover messages for a channel
     *
     * @param channelID ID of the channel to recover messages
     */
    public InboundMessageRecoverEvent(UUID channelID) {
        this.channelID = channelID;
    }


    /**
     * Get the event type
     *
     * @return event type as a string
     */
    public String getType() {
        return EVENT_TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateState() throws AndesException {
        AndesSubscription subscription = andesSubscriptionManager.getSubscriptionByProtocolChannel(channelID);
        subscription.recoverMessages();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String eventInfo() {
        return EVENT_TYPE;
    }

    /**
     * Prepare event for recovering messages
     *
     * @param andesSubscriptionManager AndesSubscriptionManager for managing subscriptions
     */
    public void prepareForMessageRecovery(AndesSubscriptionManager andesSubscriptionManager) {
        this.andesSubscriptionManager = andesSubscriptionManager;
    }

}
