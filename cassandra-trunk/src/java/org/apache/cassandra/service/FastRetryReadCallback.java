package org.apache.cassandra.service;
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


import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy.LocalReadRunnable;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FastRetryReadCallback<T> extends ReadCallback<T>
{

    protected static final Logger logger = LoggerFactory.getLogger( FastRetryReadCallback.class );
	private InetAddress retryEndpoint;

    public FastRetryReadCallback(IResponseResolver resolver, ConsistencyLevel consistencyLevel, IReadCommand command, List<InetAddress> endpoints)
    {
        super(resolver, consistencyLevel, command, endpoints);
    }
    
    @Override
    public T get() throws TimeoutException, DigestMismatchException, IOException
    {
    	long fastRetry = DatabaseDescriptor.getFastRetryTimeout() - (System.currentTimeMillis() - startTime);
        boolean success;
        try
        {
            success = condition.await(fastRetry, TimeUnit.MILLISECONDS);
            if (!success)
            {
                logger.debug("--- fast timeout expired, retrying");
                MessagingService.instance().sendRR((ReadCommand) command, retryEndpoint, this);
            	long timeout = DatabaseDescriptor.getRpcTimeout() - (System.currentTimeMillis() - startTime);
            	success = condition.await(timeout, TimeUnit.MILLISECONDS);
            }
        }
        catch (InterruptedException ex)
        {
            logger.debug("--- InterruptedException in fast retry callback");
            throw new AssertionError(ex);
        }

        if (!success)
        {
            StringBuilder sb = new StringBuilder("");
            for (Message message : resolver.getMessages())
                sb.append(message.getFrom()).append(", ");
            logger.debug("--- TimeoutException in fast retry callback");
            throw new TimeoutException("Operation timed out - received only " + received.get() + " responses from " + sb.toString() + " .");
        }

        return blockfor == 1 ? resolver.getData() : resolver.resolve();
    }
    
    public void setEndpoint(InetAddress endpoint)
    {
    	retryEndpoint = endpoint;
    }

}
