/*-
 * #%L
 * hive
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2019 jzdayz
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package io.github.jzdayz.route.anything.hive.context;

import io.github.jzdayz.route.anything.hive.connection.Connection;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.EnvironmentConfiguration;
import org.apache.commons.configuration2.SystemConfiguration;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.thrift.transport.TSocket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Context {

    public final static CompositeConfiguration CONFIGURATION = new CompositeConfiguration();
    public static final List<Connection> CONNECTIONS = new ArrayList<>();
    /**
     *  如果client超时时间为0，那么需要用客户端判断是否关闭，判断连接驱逐
     */
    public static final Map<Connection, TSocket> CLIENT_CONNECTION = new ConcurrentHashMap<>();

    public static final Map<TSessionHandle,Connection> CLI_SESSION_CON = new ConcurrentHashMap<>(128);
    public static final Map<Connection,TSessionHandle> CON_CLI_SESSION = new ConcurrentHashMap<>(128);

    public static final Map<TOperationHandle,Connection> CLI_OPERATION_CON = new ConcurrentHashMap<>(256);
    public static final Map<Connection, Set<TOperationHandle>> CON_CLI_OPERATION = new ConcurrentHashMap<>(128);


    static {
        CONFIGURATION.addConfiguration(new EnvironmentConfiguration());
        CONFIGURATION.addConfiguration(new SystemConfiguration());
    }

    public static Configuration getConfiguration(){
        return CONFIGURATION;
    }
}
