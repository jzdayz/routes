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
package io.github.jzdayz.route.anything.hive;

import io.github.jzdayz.route.anything.hive.auth.RouteDefinition;
import io.github.jzdayz.route.anything.hive.auth.UserPwdAuth;
import io.github.jzdayz.route.anything.hive.config.ConfigNames;
import io.github.jzdayz.route.anything.hive.connection.Connection;
import io.github.jzdayz.route.anything.hive.connection.SimpleIface;
import io.github.jzdayz.route.anything.hive.connection.Work;
import io.github.jzdayz.route.anything.hive.context.Context;
import io.github.jzdayz.route.anything.hive.service.TSetIpClientAddressProcessor;
import io.github.jzdayz.server.SseApplication;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.configuration2.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Map;

@Slf4j
public class HiveRoute {
    public static void main(String[] args) throws Exception {
        Configuration configuration = Context.getConfiguration();
        {
            new Thread(() -> SseApplication.doReportServer(new Object() {
                                                               @Override
                                                               public String toString() {
                                                                   return (String) doResolve();
                                                               }
                                                           },
                    configuration.getInt(ConfigNames.WEB_PORT, 8080))).start();
        }

        RouteDefinition.parse(RouteDefinition.DEFAULT_CONFIG);
        SimpleIface simpleIface = new SimpleIface();
        TSetIpClientAddressProcessor processor = new TSetIpClientAddressProcessor(simpleIface);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION.varname, "CUSTOM");
        System.setProperty(HiveConf.ConfVars.HIVE_SERVER2_CUSTOM_AUTHENTICATION_CLASS.varname, UserPwdAuth.class.getName());
        TThreadPoolServer.Args arg =
                new TThreadPoolServer.Args(new TServerSocket(
                        configuration.getInt(ConfigNames.SERVER_PORT, 20001),
                        configuration.getInt(ConfigNames.CLIENT_TIMEOUT,/*一分钟 60_000*/0)))
                        .protocolFactory(new TBinaryProtocol.Factory())
                        .minWorkerThreads(configuration.getInt(ConfigNames.SERVER_WORKER_MIN_THREADS, 1))
                        .maxWorkerThreads(configuration.getInt(ConfigNames.SERVER_WORKER_MAX_THREADS, 100))
                        .transportFactory(new HiveAuthFactory(hiveConf).getAuthTransFactory())
                        .processorFactory(new TProcessorFactory(null) {
                            @Override
                            public TProcessor getProcessor(TTransport trans) {
                                return processor;
                            }
                        });
        TThreadPoolServer tThreadPoolServer = new TThreadPoolServer(arg);
        log.info("server starting ... ");
        Runtime.getRuntime().addShutdownHook(new Thread(Work.HOOK));
        tThreadPoolServer.serve();
    }

    private static Object doResolve() {
        Map<TSessionHandle, Connection> seSon = Context.CLI_SESSION_CON;
        Map<TOperationHandle, Connection> cliCon = Context.CLI_OPERATION_CON;
        
        StringBuilder sb = new StringBuilder();
        for (Connection value : seSon.values()) {
            sb.append(value).append("\n");
        }
        return String.format("活跃连接数: %s , 活跃操作数: %s , 活跃连接详情 : \n%s\n\n",
                seSon.size(),
                cliCon.size(),
                sb);
    }
}
