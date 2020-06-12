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

import io.github.jzdayz.route.anything.hive.auth.RouteDefinition;
import io.github.jzdayz.route.anything.hive.config.ConfigNames;
import io.github.jzdayz.route.anything.hive.context.Context;
import io.github.jzdayz.route.anything.hive.ex.ClientClosedException;
import io.github.jzdayz.route.anything.hive.ex.RouteNotFoundException;
import io.github.jzdayz.route.anything.hive.service.TSetIpClientAddressProcessor;
import io.github.jzdayz.route.anything.hive.utils.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.hive.service.cli.HandleIdentifier;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;

import java.net.SocketException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.jzdayz.route.anything.hive.context.Context.*;

/**
 *  proxy 处理对象
 *  这里进行中转请求
 */
@SuppressWarnings("DuplicatedCode")
@Slf4j
public class SimpleIface implements TCLIService.Iface {

    private final static TStatus SUCCESS = new TStatus(TStatusCode.SUCCESS_STATUS);

    static {
        long delay = Context.getConfiguration().getLong(ConfigNames.BACK_CLEAN_DELAY,10);
        AtomicInteger i = new AtomicInteger(1);
        Executors.newSingleThreadScheduledExecutor((runnable)->{
            Thread thread = new Thread(runnable);
            thread.setName(String.format(" Cleaner for Connection -- %s ",i.getAndIncrement()));
            return thread;
        }).scheduleWithFixedDelay(Work.CLEANER,delay,delay, TimeUnit.SECONDS);
        log.info(" init cleaner connections ... ");
    }



    @Override
    public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
        String userName = TSetIpClientAddressProcessor.getUserName();
        RouteDefinition routeDefinition = RouteDefinition.routes.get(
                RouteDefinition.User.builder().username(
                        Objects.requireNonNull(userName,"username")).password(
                                Objects.requireNonNull(RouteDefinition.users.get(userName),"password")).build()
        );
        if (routeDefinition == null){
            throw new RouteNotFoundException();
        }
        Connection connection = Connection.getConnection(req, routeDefinition);
        TOpenSessionResp openSessionResp = connection.getOpenSessionResp();
        // 替换sessionHandle
        TSessionHandle tSessionHandle = new TSessionHandle(new HandleIdentifier().toTHandleIdentifier());
        openSessionResp.setSessionHandle(tSessionHandle);
        connection.setOpenSessionResp(null);
        CLI_SESSION_CON.put(tSessionHandle,connection);
        CON_CLI_SESSION.put(connection,tSessionHandle);
        String userIpAddress = TSetIpClientAddressProcessor.getUserIpAddress();
        TSocket client = TSetIpClientAddressProcessor.getClient();
        int soTimeout = 0;
        try {
            soTimeout = client.getSocket().getSoTimeout();
        } catch (SocketException e) {
            log.error(" client closed ... ",e);
            throw new ClientClosedException();
        }

        log.info(" open session for ( ip : {} , user : {} ) , timeout ( client -> proxy : {} , proxy ->server : {} )", userIpAddress,userName, soTimeout,connection.getTimeout());

        connection.setOriginHost(userIpAddress);
        connection.setOriginUser(userName);

        synchronized (CONNECTIONS) {
            CONNECTIONS.add(connection);
        }
        return openSessionResp;
    }

    @Override
    public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TCloseSessionResp resp = connection.getClient().CloseSession(req);
        Work.expulsion(connection,true);
        return resp;
    }

    @Override
    public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        return connection.getClient().GetInfo(req);
    }

    @Override
    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TExecuteStatementResp resp = connection.getClient().ExecuteStatement(req);
        try {
            Utils.verifySuccessWithInfo(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetTypeInfoResp resp = connection.getClient().GetTypeInfo(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetCatalogsResp resp = connection.getClient().GetCatalogs(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetSchemasResp resp = connection.getClient().GetSchemas(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetTablesResp resp = connection.getClient().GetTables(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetTableTypesResp resp = connection.getClient().GetTableTypes(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetColumnsResp resp = connection.getClient().GetColumns(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetFunctionsResp resp = connection.getClient().GetFunctions(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetPrimaryKeysResp resp = connection.getClient().GetPrimaryKeys(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TGetCrossReferenceResp resp = connection.getClient().GetCrossReference(req);
        try {
            Utils.verifySuccess(resp.getStatus());
        } catch (Exception e){
            return resp;
        }
        CLI_OPERATION_CON.put(resp.getOperationHandle(),connection);
        CON_CLI_OPERATION.computeIfAbsent(connection,(con)-> new HashSet<>()).add(resp.getOperationHandle());
        return resp;
    }

    @Override
    public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
        Connection connection = CLI_OPERATION_CON.get(req.getOperationHandle());
        return connection.getClient().GetOperationStatus(req);
    }

    @Override
    public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
        Connection connection = CLI_OPERATION_CON.get(req.getOperationHandle());
        return connection.getClient().CancelOperation(req);
    }

    @Override
    public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
        TOperationHandle operationHandle = req.getOperationHandle();
        Connection connection = CLI_OPERATION_CON.get(operationHandle);
        TCloseOperationResp resp = connection.getClient().CloseOperation(req);
        CLI_OPERATION_CON.remove(operationHandle);
        Set<TOperationHandle> tOperationHandles = CON_CLI_OPERATION.get(connection);
        if (tOperationHandles!=null) {
            tOperationHandles.remove(operationHandle);
            if (tOperationHandles.size() == 0){
                CON_CLI_OPERATION.remove(connection);
            }
        }else{
            log.warn(" no operation should be close ");
        }
        operationHandle.clear();
        return resp;
    }

    @Override
    public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req) throws TException {
        Connection connection = CLI_OPERATION_CON.get(req.getOperationHandle());
        return connection.getClient().GetResultSetMetadata(req);
    }

    @Override
    public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
        Connection connection = CLI_OPERATION_CON.get(req.getOperationHandle());
        return connection.getClient().FetchResults(req);
    }

    @Override
    public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        return connection.getClient().GetDelegationToken(req);
    }

    @Override
    public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        return connection.getClient().CancelDelegationToken(req);
    }

    @Override
    public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        return connection.getClient().RenewDelegationToken(req);
    }

    @Override
    public TGetQueryIdResp GetQueryId(TGetQueryIdReq req) throws TException {
        Connection connection = CLI_OPERATION_CON.get(req.getOperationHandle());
        return connection.getClient().GetQueryId(req);
    }

    @Override
    public TSetClientInfoResp SetClientInfo(TSetClientInfoReq req) throws TException {
        Connection connection = CLI_SESSION_CON.get(req.getSessionHandle());
        req.setSessionHandle(connection.getSessionHandle());
        TSetClientInfoResp tSetClientInfoResp = null;
        try {
            tSetClientInfoResp = connection.getClient().SetClientInfo(req);
        } catch (TException e) {
            tSetClientInfoResp = new TSetClientInfoResp();
            tSetClientInfoResp.setStatus(SUCCESS);
        }
        return tSetClientInfoResp;
    }
}
