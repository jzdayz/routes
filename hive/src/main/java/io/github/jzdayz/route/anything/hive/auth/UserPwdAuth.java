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

import io.github.jzdayz.route.anything.hive.ex.NoRouteDefinitionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.hive.service.auth.PasswdAuthenticationProvider;

import javax.security.sasl.AuthenticationException;
import java.util.HashSet;
import java.util.Set;

@Slf4j
public class UserPwdAuth implements PasswdAuthenticationProvider {

    private boolean init;
    private Set<RouteDefinition.User> user = new HashSet<>();

    public void init(){
        if(RouteDefinition.routes.size()<=0){
            throw new NoRouteDefinitionException();
        }
        user.addAll(RouteDefinition.routes.keySet());
        init = true;
    }

    @Override
    public void Authenticate(String user, String password) throws AuthenticationException {
        if (!init){
            init();
        }
        if (!this.user.contains(RouteDefinition.User.builder().username(user).password(password).build())){
            log.info("login error for ( username : {} , password : {} )",user,password);
            log.info("the effective user is : {} ",RouteDefinition.users);
            throw new AuthenticationException();
        }
        log.info("login success for ( username : {} , password : {} )",user,password);
    }
}
