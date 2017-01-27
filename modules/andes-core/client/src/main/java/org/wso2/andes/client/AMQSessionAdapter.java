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

import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.RemoteException;

public class AMQSessionAdapter
{
    protected AMQSession _session;

    public AMQSession getSession() {
        return _session;
    }

    protected void checkValidDestination(Destination destination) throws InvalidDestinationException {
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
                        connection.stop();
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
    }

    private String getNodeForDestination(Destination destination) throws ServiceException, MalformedURLException,
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
    }
}
