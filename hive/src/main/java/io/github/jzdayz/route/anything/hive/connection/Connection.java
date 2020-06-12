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
package io.github.jzdayz.route.anything.hive.connection;

import com.google.common.collect.ImmutableMap;
import io.github.jzdayz.route.anything.hive.auth.RouteDefinition;
import io.github.jzdayz.route.anything.hive.context.Context;
import io.github.jzdayz.route.anything.hive.ex.CannotGetTransportException;
import io.github.jzdayz.route.anything.hive.ex.CannotOpenSessionException;
import io.github.jzdayz.route.anything.hive.ex.ClientClosedException;
import io.github.jzdayz.route.anything.hive.service.TSetIpClientAddressProcessor;
import io.github.jzdayz.route.anything.hive.thrift.TransportFactory;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.StringJoiner;

/**
 * 连接对象
 */
@Builder
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class Connection {

    private String originHost;
    private String toHost;
    private String originUser;
    private String toUser;
    private String toUserPwd;
    // 过期时间
    private long expireDate;
    private long timeout;
    private long clientTimeout;

    private TTransport transport;
    private TCLIService.Iface client;
    private TSessionHandle sessionHandle;
    private boolean isClosed = true;
    private TProtocolVersion protocolClient;
    private TProtocolVersion protocolServer;
    private TOpenSessionResp openSessionResp;

    public void clear() {
        originHost = null;
        toHost = null;
        originUser = null;
        toUser = null;
        toUserPwd = null;

        if (transport != null) {
            transport.close();
        }
        transport = null;
        client = null;
        sessionHandle = null;
        isClosed = true;
        protocolClient = null;
        protocolServer = null;
        openSessionResp = null;
    }

    public static Connection getConnection(TOpenSessionReq req, RouteDefinition routeDefinition) {
        ImmutableMap<String, String> conf = ImmutableMap.of(Utils.JdbcConnectionParams.AUTH_USER, routeDefinition.getUser().getUsername(),
                Utils.JdbcConnectionParams.AUTH_PASSWD, routeDefinition.getUser().getPassword());
        // get transport
        TTransport transport = null;
        Connection connection = new Connection();
        int clientTimeout = 0;
        // proxy->realServer过期时间为 client->proxy+1分钟
        try {
            TSocket client = TSetIpClientAddressProcessor.getClient();
            clientTimeout = client.getSocket().getSoTimeout();
            // 如果client timeout为0 ，那么proxy也为0
            if (clientTimeout!=0) {
                connection.setClientTimeout(clientTimeout);
                connection.setTimeout(connection.getClientTimeout() + 10 * 1000);
                connection.setExpireDate( connection.getTimeout()
                        + System.currentTimeMillis() );
            }else{
                Context.CLIENT_CONNECTION.put(connection,client);
            }
        } catch (Exception e) {
            if(e instanceof NullPointerException){
                throw (NullPointerException)e;
            }
            // client is closed
            throw new ClientClosedException();
        }
        try {
            transport = TransportFactory.getTTransport(routeDefinition.getHost(),
                    routeDefinition.getPort(), clientTimeout, conf);
        } catch (Exception e) {
            log.error(" get transport ", e);
            throw new CannotGetTransportException();
        }
        connection.setTransport(transport);
        connection.setToHost(routeDefinition.getHost());
        connection.setToUser(routeDefinition.getVisitUsername());
        connection.setToUserPwd(routeDefinition.getVisitPassword());
        connection.setClient(new TCLIService.Client(new TBinaryProtocol(transport)));
        connection.setClosed(false);
        try {
            openSession(req, connection, routeDefinition);
        } catch (Exception e) {
            log.error("open session ", e);
            throw new CannotOpenSessionException();
        }
        return connection;
    }

    private static void openSession(TOpenSessionReq req, Connection connection, RouteDefinition routeDefinition) throws Exception {

        req.setUsername(routeDefinition.getUser().getUsername());
        req.setPassword(routeDefinition.getUser().getPassword());
        connection.setProtocolClient(req.getClient_protocol());
        // todo proxy to server auth type?

        TOpenSessionResp resp = connection.getClient().OpenSession(req);
        io.github.jzdayz.route.anything.hive.utils.Utils.verifySuccess(resp.getStatus());
        connection.setProtocolServer(resp.getServerProtocolVersion());
        connection.setSessionHandle(resp.getSessionHandle());
        connection.setOpenSessionResp(resp);
    }
    @Override
    public String toString() {
        return new StringJoiner(", ", Connection.class.getSimpleName() + "[", "]")
                .add("originHost='" + originHost + "'")
                .add("toHost='" + toHost + "'")
                .add("originUser='" + originUser + "'")
                .add("toUser='" + toUser + "'")
                .add("toUserPwd='" + toUserPwd + "'")
                .add("expireDate=" + expireDate)
                .add("timeout=" + timeout)
                .add("clientTimeout=" + clientTimeout)
                .toString()+"\n";
    }
}
