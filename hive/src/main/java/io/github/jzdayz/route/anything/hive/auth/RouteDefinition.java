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
package io.github.jzdayz.route.anything.hive.auth;

import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Setter
@Getter
@Slf4j
@NoArgsConstructor
@ToString
public class RouteDefinition {

    public static final Map<User, RouteDefinition> routes = new ConcurrentHashMap<>();
    // username - password
    public static final Map<String,String> users = new ConcurrentHashMap<>();
    public static final String DEFAULT_CONFIG = "config.yml";

    private User user;
    private String host;
    private int port;
    private String visitUsername;
    private String visitPassword;

    @Setter
    @Getter
    @EqualsAndHashCode
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User{
        private String username;
        private String password;
    }

    public static void parse(String resource){
        resource = resource == null ? DEFAULT_CONFIG : resource;
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = RouteDefinition.class.getClassLoader().getResourceAsStream(resource);
            Yaml yaml = new Yaml();
            RouteDefinition[] routeDefinitions = yaml.loadAs(resourceAsStream, RouteDefinition[].class);
            routes.putAll(Stream.of(routeDefinitions).collect(Collectors.toMap(RouteDefinition::getUser, Function.identity())));
            users.putAll(routes.keySet().stream().collect(Collectors.toMap(User::getUsername, User::getPassword,(x,y)->{
                throw new RuntimeException("username must be not same");
            })));
        } catch (Exception e) {
            log.error(" error open config file ",e);
        } finally {
            if (resourceAsStream!=null){
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    log.error("close file ",e);
                }
            }
        }

    }

}
