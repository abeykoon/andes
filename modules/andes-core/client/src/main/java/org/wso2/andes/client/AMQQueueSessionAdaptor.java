/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.wso2.andes.client;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.wso2.andes.AMQException;
import org.wso2.andes.jms.BrokerDetails;
import org.wso2.andes.jms.ConnectionURL;

import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.rmi.RemoteException;

import javax.jms.*;
import javax.jms.IllegalStateException;
import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

/**
 * Need this adaptor class to conform to JMS spec and throw IllegalStateException
 * from createDurableSubscriber, unsubscribe, createTopic & createTemporaryTopic
 */
public class AMQQueueSessionAdaptor extends AMQSessionAdapter implements QueueSession {
    //holds a session for delegation
//    protected AMQSession _session;

    /**
     * Construct an adaptor with a session to wrap
     *
     * @param session
     */
    public AMQQueueSessionAdaptor(Session session) {
        _session = (AMQSession) session;
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return _session.createTemporaryQueue();
    }

    public Queue createQueue(String string) throws JMSException {
        return _session.createQueue(string);
    }

    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return _session.createReceiver(queue);
    }

    public QueueReceiver createReceiver(Queue queue, String string) throws JMSException {
        return _session.createReceiver(queue, string);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        checkValidDestination(queue);
        return _session.createSender(queue);
    }

    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return _session.createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String string) throws JMSException {
        return _session.createBrowser(queue, string);
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return _session.createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return _session.createMapMessage();
    }

    public Message createMessage() throws JMSException {
        return _session.createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return _session.createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return _session.createObjectMessage(serializable);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return _session.createStreamMessage();
    }

    public TextMessage createTextMessage() throws JMSException {
        return _session.createTextMessage();
    }

    public TextMessage createTextMessage(String string) throws JMSException {
        return _session.createTextMessage(string);
    }

    public boolean getTransacted() throws JMSException {
        return _session.getTransacted();
    }

    public int getAcknowledgeMode() throws JMSException {
        return _session.getAcknowledgeMode();
    }

    public void commit() throws JMSException {
        _session.commit();
    }

    public void rollback() throws JMSException {
        _session.rollback();
    }

    public void close() throws JMSException {
        _session.close();
    }

    public void recover() throws JMSException {
        _session.recover();
    }

    public MessageListener getMessageListener() throws JMSException {
        return _session.getMessageListener();
    }

    public void setMessageListener(MessageListener messageListener) throws JMSException {
        _session.setMessageListener(messageListener);
    }

    public void run() {
        _session.run();
    }

    public MessageProducer createProducer(Destination destination) throws JMSException {
        return _session.createProducer(destination);
    }

    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        checkValidDestination(destination);
        return _session.createConsumerImpl(destination, _session.getDefaultPrefetchHigh(),
                                           _session.getDefaultPrefetchLow(), false,
                                           (destination instanceof Topic), null, null,
                ((destination instanceof AMQDestination) && ((AMQDestination) destination).isBrowseOnly()), false);

    }

    public MessageConsumer createConsumer(Destination destination, String string) throws JMSException {
        return _session.createConsumer(destination, string);
    }

    public MessageConsumer createConsumer(Destination destination, String string, boolean b) throws JMSException {
        return _session.createConsumer(destination, string, b);
    }

    //The following methods cannot be called from a QueueSession as per JMS spec

    public Topic createTopic(String string) throws JMSException {
        throw new IllegalStateException("Cannot call createTopic from QueueSession");
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String string) throws JMSException {
        throw new IllegalStateException("Cannot call createDurableSubscriber from QueueSession");
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String string, String string1, boolean b)
            throws JMSException {
        throw new IllegalStateException("Cannot call createDurableSubscriber from QueueSession");
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new IllegalStateException("Cannot call createTemporaryTopic from QueueSession");
    }

    public void unsubscribe(String string) throws JMSException {
        throw new IllegalStateException("Cannot call unsubscribe from QueueSession");
    }

    /*public AMQSession getSession() {
        return _session;
    }*/

    /*private void checkValidDestination(Destination destination) throws InvalidDestinationException {
        if (destination == null) {
            throw new javax.jms.InvalidDestinationException("Invalid Queue");
        } else {
            try {
                String nodeForDesitnation = getNodeForDestination(destination);
                String[] hostPort = nodeForDesitnation.split(":");
                AMQConnection connection = this._session._connection;
                String host = connection.getActiveBrokerDetails().getHost();
                int port = connection.getActiveBrokerDetails().getPort();
                String matchinHost = hostPort[0];
                int matchingPort = Integer.parseInt(hostPort[1]);
                if (!host.equals(matchinHost)
                    || matchingPort != port) {
                    if (connection.getSessions().size() == 1) {
                        connection.close();
                        System.out.println("Redirected connection to: " + nodeForDesitnation);
                        BrokerDetails brokerDetails = new AMQBrokerDetails(matchinHost, matchingPort, connection
                                .getSSLConfiguration());
                        brokerDetails.setTransport(connection.getActiveBrokerDetails().getTransport());
                        ConnectionURL url = connection.getConnectionURL();
                        url.getAllBrokerDetails().add(0, brokerDetails);
                        connection.reInitAMQConnection(url);
                        if (!connection._closed.getAndSet(false)) {
                            connection._closing.set(false);
                        }
                        this._session = (AMQSession) connection.createSession(false, 1);
                    } else
                        throw new InvalidDestinationException(
                                "Invalid node: " + host + ":" + port + " for destination. "
                                + "Matching node for destination: "
                                + destination + " is " + nodeForDesitnation);

                }
            } catch (ServiceException | JMSException | IOException | AMQException e) {
                //TODO just only for the POC
                throw new InvalidDestinationException(e.getMessage());
            }
        }
    }*/

    /*private String getNodeForDestination(Destination destination) throws ServiceException, MalformedURLException,
            RemoteException, JMSException {
        String host = this._session._connection.getActiveBrokerDetails().getHost();
        String port = "9443";
        String endpoint = "https://" + host + ":" + port + "/services/AndesManagerService";
        System.setProperty(
                "javax.net.ssl.trustStore",
                "/home/sasikala/Documents/MB/Cluster/MB1/wso2mb-3.2.0-SNAPSHOT/repository/resources/security"
                + "/wso2carbon.jks");
        Service service = new Service();
        Call call = (Call) service.createCall();
        call.setTargetEndpointAddress(new java.net.URL(endpoint));
        call.setOperationName(new QName("http://mgt.cluster.andes.carbon.wso2.org", "getOwningNodeOfQueue"));
        call.setUsername("admin");
        call.setPassword("admin");

        String response = (String) call.invoke(new Object[]{((Queue) destination).getQueueName(), "amqp"});
        return response;
    }*/
}
