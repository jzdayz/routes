/*-
 * #%L
 * es-gateway
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

package io.github.jzdayz.scgatewayes.auth.simple.invalid;

import io.netty.handler.codec.http.HttpHeaderNames;
import org.springframework.cloud.gateway.handler.predicate.AbstractRoutePredicateFactory;
import org.springframework.cloud.gateway.handler.predicate.GatewayPredicate;
import org.springframework.http.HttpHeaders;
import org.springframework.util.CollectionUtils;
import org.springframework.web.server.ServerWebExchange;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class InvalidRoutePredicateFactory
        extends AbstractRoutePredicateFactory<InvalidRoutePredicateFactory.Config> {

    public static final Set<String> VALID = new HashSet<>();

    public InvalidRoutePredicateFactory() {
        super(Config.class);
    }

    @Override
    public Predicate<ServerWebExchange> apply(Config config) {

        return new GatewayPredicate() {
            @Override
            public boolean test(ServerWebExchange exchange) {
                HttpHeaders headers = exchange.getRequest().getHeaders();
                List<String> values = headers.get(HttpHeaderNames.AUTHORIZATION.toString());
                if (CollectionUtils.isEmpty(values)){
                    return false;
                }
                String auth = values.get(0);
                return !VALID.contains(auth);
            }

            @Override
            public String toString() {
                return super.toString();
            }
        };
    }

    public static class Config {


    }

}
