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
package io.github.jzdayz.route.anything.hive.thrift;


import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hive.jdbc.Utils;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.hive.service.cli.session.SessionUtils;
import org.apache.thrift.transport.TTransport;

import javax.security.sasl.Sasl;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 *  Create http or binary transport
 *
 *  todo http
 */

@Slf4j
public class TransportFactory {

    /**
     *  与jdbc 的行为一致
     */
    public static TTransport getTTransport(String host, int port, int loginTimeout, Map<String,String> conf) throws Exception{

        TTransport transport = null;
        try {
            TTransport socketTransport = HiveAuthUtils.getSocketTransport(host, port, loginTimeout);
            // handle secure connection if specified
            if (!Utils.JdbcConnectionParams.AUTH_SIMPLE.equals(conf.get(Utils.JdbcConnectionParams.AUTH_TYPE))) {
                // If Kerberos
                Map<String, String> saslProps = new HashMap<String, String>();
                SaslQOP saslQOP = SaslQOP.AUTH;
                if (conf.containsKey(Utils.JdbcConnectionParams.AUTH_QOP)) {
                    try {
                        saslQOP = SaslQOP.fromString(conf.get(Utils.JdbcConnectionParams.AUTH_QOP));
                    } catch (IllegalArgumentException e) {
                        throw new SQLException("Invalid " + Utils.JdbcConnectionParams.AUTH_QOP +
                                " parameter. " + e.getMessage(), "42000", e);
                    }
                    saslProps.put(Sasl.QOP, saslQOP.toString());
                } else {
                    // If the client did not specify qop then just negotiate the one supported by server
                    saslProps.put(Sasl.QOP, "auth-conf,auth-int,auth");
                }
                saslProps.put(Sasl.SERVER_AUTH, "true");
                String tokenStr = null;
                if (Utils.JdbcConnectionParams.AUTH_TOKEN.equals(conf.get(Utils.JdbcConnectionParams.AUTH_TYPE))) {
                    // If there's a delegation token available then use token based connection
                    tokenStr = getClientDelegationToken(conf);
                }
                if (tokenStr != null) {
                    transport = KerberosSaslHelper.getTokenTransport(tokenStr,
                            host, socketTransport, saslProps);
                } else if(conf.containsKey(Utils.JdbcConnectionParams.AUTH_PRINCIPAL)){
                    transport = KerberosSaslHelper.getKerberosTransport(
                            conf.get(Utils.JdbcConnectionParams.AUTH_PRINCIPAL), host,
                            socketTransport, saslProps, Utils.JdbcConnectionParams.AUTH_KERBEROS_AUTH_TYPE_FROM_SUBJECT.equals(conf
                                    .get(Utils.JdbcConnectionParams.AUTH_KERBEROS_AUTH_TYPE)));
                } else {
                    // we are using PLAIN Sasl connection with user/password
                    String userName = conf.get(Utils.JdbcConnectionParams.AUTH_USER);
                    String passwd = conf.get(Utils.JdbcConnectionParams.AUTH_PASSWD);;
                    // Overlay the SASL transport on top of the base socket transport (SSL or non-SSL)
                    transport = PlainSaslHelper.getPlainTransport(userName, passwd, socketTransport);
                }
            } else {
                // Raw socket connection (non-sasl)
                transport = socketTransport;
            }
        } catch (Exception e) {
            log.error("create transport error ",e);
        }

        Objects.requireNonNull(transport," transport not be null ");

        if (!transport.isOpen()){
            transport.open();
        }

        log.info("connected to {} port {} ",host,port);

        return transport;

    }


    private static String getClientDelegationToken(Map<String, String> jdbcConnConf) throws SQLException {
        String tokenStr = null;
        if (!Utils.JdbcConnectionParams.AUTH_TOKEN.equalsIgnoreCase(jdbcConnConf.get(Utils.JdbcConnectionParams.AUTH_TYPE))) {
            return null;
        }
        DelegationTokenFetcher fetcher = new DelegationTokenFetcher();
        try {
            tokenStr = fetcher.getTokenStringFromFile();
        } catch (IOException e) {
            log.warn("Cannot get token from environment variable $HADOOP_TOKEN_FILE_LOCATION=" +
                    System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION));
        }
        if (tokenStr == null) {
            try {
                return fetcher.getTokenFromSession();
            } catch (IOException e) {
                throw new SQLException("Error reading token ", e);
            }
        }
        return tokenStr;
    }

    static class DelegationTokenFetcher {
        String getTokenStringFromFile() throws IOException {
            if (System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION) == null) {
                return null;
            }
            Credentials cred = new Credentials();
            try (DataInputStream dis = new DataInputStream(new FileInputStream(System.getenv(UserGroupInformation
                    .HADOOP_TOKEN_FILE_LOCATION)))) {
                cred.readTokenStorageStream(dis);
            }
            return getTokenFromCredential(cred, "hive");
        }

        String getTokenFromCredential(Credentials cred, String key) throws IOException {
            Token<? extends TokenIdentifier> token = cred.getToken(new Text(key));
            if (token == null) {
                log.warn("Delegation token with key: [hive] cannot be found.");
                return null;
            }
            return token.encodeToUrlString();
        }

        String getTokenFromSession() throws IOException {
            log.debug("Fetching delegation token from session.");
            return SessionUtils.getTokenStrForm(HiveAuthConstants.HS2_CLIENT_TOKEN);
        }
    }


}
