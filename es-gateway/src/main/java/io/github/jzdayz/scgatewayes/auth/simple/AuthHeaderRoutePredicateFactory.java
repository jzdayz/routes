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

package io.github.jzdayz.scgatewayes.auth.simple;

import io.github.jzdayz.scgatewayes.auth.simple.invalid.InvalidRoutePredicateFactory;
import io.netty.handler.codec.http.HttpHeaderNames;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gateway.handler.predicate.AbstractRoutePredicateFactory;
import org.springframework.cloud.gateway.handler.predicate.GatewayPredicate;
import org.springframework.http.HttpHeaders;
import org.springframework.util.Assert;
import org.springframework.util.Base64Utils;
import org.springframework.util.CollectionUtils;
import org.springframework.web.server.ServerWebExchange;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class AuthHeaderRoutePredicateFactory
        extends AbstractRoutePredicateFactory<AuthHeaderRoutePredicateFactory.Config> {

    public AuthHeaderRoutePredicateFactory() {
        super(Config.class);
    }

    private final static String SYMBOL = "-";

    @Override
    public ShortcutType shortcutType() {
        return ShortcutType.GATHER_LIST;
    }

    @Override
    public List<String> shortcutFieldOrder() {
        return Collections.singletonList("users");
    }

    @Override
    public Predicate<ServerWebExchange> apply(Config config) {

        // 没有验证数据准确性
        config.getValid().addAll(config.getUsers().stream().map(k->{
            int i = k.indexOf(SYMBOL);
            Assert.isTrue(i>0,"配置不符合要求，必须是name-pwd");
            String name = k.substring(0,i);
            String pwd = k.substring(i+1);
            return "Basic "+ Base64Utils.encodeToString((name+":"+pwd).getBytes());
        }).collect(Collectors.toSet()));
        InvalidRoutePredicateFactory.VALID.addAll(config.getValid());

        return new GatewayPredicate() {
            @Override
            public boolean test(ServerWebExchange exchange) {
                HttpHeaders headers = exchange.getRequest().getHeaders();
                List<String> values = headers.get(HttpHeaderNames.AUTHORIZATION.toString());
                if (CollectionUtils.isEmpty(values)){
                    return false;
                }
                String auth = values.get(0);
                return config.getValid().contains(auth);
            }

            @Override
            public String toString() {
                return super.toString();
            }
        };
    }

    @Data
    public static class Config {

        private List<String> users = new ArrayList<>();
        private Set<String> valid = new HashSet<>();

    }

}
