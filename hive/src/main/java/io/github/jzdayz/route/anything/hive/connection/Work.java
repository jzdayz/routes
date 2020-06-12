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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hive.service.rpc.thrift.TOperationHandle;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.thrift.transport.TSocket;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import static io.github.jzdayz.route.anything.hive.context.Context.*;

@Slf4j
public class Work{

    public static final Cleaner CLEANER = new Cleaner();
    public static final Hook HOOK = new Hook();

    private static class Hook implements Runnable{

        @Override
        public void run() {
            checkOutConnections(true);
        }
    }

    private static class Cleaner implements Runnable{
        @Override
        public void run() {
            checkOutConnections(false);
        }
    }

    private static void checkOutConnections(boolean justEx) {
        try {
            if (CONNECTIONS.size()>0){
                Set<Connection> needDelete = new HashSet<>();
                for (Connection next : CONNECTIONS) {
                    if (expulsion(next, justEx)) {
                        needDelete.add(next);
                    }
                }
                if (needDelete.size()>0){
                    synchronized (CONNECTIONS){
                        CONNECTIONS.removeAll(needDelete);
                    }
                }
            }
        }catch (Exception e){
            log.error(" cleaner error ... ",e);
        }
    }

    public static boolean expulsion(Connection connection,boolean justEx){
        TSocket tSocket = CLIENT_CONNECTION.get(connection);
        if (connection.getExpireDate()<System.currentTimeMillis() && connection.getExpireDate()!=0
                || justEx
                || connection.getExpireDate() == 0 && tSocket!=null && !tSocket.isOpen()){
            log.info(" turn out the connection {} , expel date [{}]",connection, DateFormatUtils.format(new Date(connection.getExpireDate()),"yyyy-MM-dd HH:mm:ss"));
            clear(connection);
            return true;
        }
        return false;
    }

    private static void clear(Connection connection) {
        CLIENT_CONNECTION.remove(connection);
        Set<TOperationHandle> tOperationHandles = CON_CLI_OPERATION.get(connection);
        TSessionHandle tSessionHandle = CON_CLI_SESSION.get(connection);

        CLI_SESSION_CON.remove(tSessionHandle);
        tSessionHandle.clear();
        if (tOperationHandles!=null) {
            tOperationHandles.forEach((op) -> {
                CLI_OPERATION_CON.remove(op);
                op.clear();
            });
        }
        CON_CLI_SESSION.remove(connection);
        CON_CLI_OPERATION.remove(connection);

        connection.clear();
    }
}
