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
package io.github.jzdayz.route.anything.hive.service;

import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TSetIpClientAddressProcessor<I extends Iface> extends TCLIService.Processor<Iface> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TSetIpClientAddressProcessor.class.getName());

  public TSetIpClientAddressProcessor(Iface iface) {
    super(iface);
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    setIpAddressAndClient(in);
    setUserName(in);
    try {
      return super.process(in, out);
    } finally {
      THREAD_LOCAL_USER_NAME.remove();
      THREAD_LOCAL_IP_ADDRESS.remove();
      THREAD_LOCAL_CLIENT.remove();
    }
  }

  private void setUserName(final TProtocol in) {
    TTransport transport = in.getTransport();
    if (transport instanceof TSaslServerTransport) {
      String userName = ((TSaslServerTransport) transport).getSaslServer().getAuthorizationID();
      THREAD_LOCAL_USER_NAME.set(userName);
    }
  }

  protected void setIpAddressAndClient(final TProtocol in) {
    TTransport transport = in.getTransport();
    TSocket tSocket = getUnderlyingSocketFromTransport(transport);
    THREAD_LOCAL_CLIENT.set(tSocket);
    if (tSocket == null) {
      LOGGER.warn("Unknown Transport, cannot determine ipAddress");
    } else {
      THREAD_LOCAL_IP_ADDRESS.set(tSocket.getSocket().getInetAddress().getHostAddress());
    }
  }

  private TSocket getUnderlyingSocketFromTransport(TTransport transport) {
    while (transport != null) {
      if (transport instanceof TSaslServerTransport) {
        transport = ((TSaslServerTransport) transport).getUnderlyingTransport();
      }
      if (transport instanceof TSaslClientTransport) {
        transport = ((TSaslClientTransport) transport).getUnderlyingTransport();
      }
      if (transport instanceof TSocket) {
        return (TSocket) transport;
      }
    }
    return null;
  }

  private static final ThreadLocal<String> THREAD_LOCAL_IP_ADDRESS = ThreadLocal.withInitial(() -> null);

  private static final ThreadLocal<String> THREAD_LOCAL_USER_NAME = ThreadLocal.withInitial(() -> null);

  private static final ThreadLocal<TSocket> THREAD_LOCAL_CLIENT = ThreadLocal.withInitial(() -> null);

  public static String getUserIpAddress() {
    return THREAD_LOCAL_IP_ADDRESS.get();
  }

  public static String getUserName() {
    return THREAD_LOCAL_USER_NAME.get();
  }

  public static TSocket getClient(){
    return THREAD_LOCAL_CLIENT.get();
  }

}
