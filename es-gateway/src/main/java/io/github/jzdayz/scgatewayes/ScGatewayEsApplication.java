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
package io.github.jzdayz.scgatewayes;

import io.github.jzdayz.scgatewayes.auth.simple.AuthHeaderRoutePredicateFactory;
import io.github.jzdayz.scgatewayes.auth.simple.invalid.InvalidGatewayFilterFactory;
import io.github.jzdayz.scgatewayes.auth.simple.invalid.InvalidRoutePredicateFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ScGatewayEsApplication {

    public static void main(String[] args){
        SpringApplication.run(ScGatewayEsApplication.class, args);
    }

    @Bean
    public Object authHeaderRoutePredicateFactory(){
        return new AuthHeaderRoutePredicateFactory();
    }

    @Bean
    public Object trueRoutePredicateFactory(){
        return new InvalidRoutePredicateFactory();
    }

    @Bean
    public Object errorGatewayFilterFactory(){
        return new InvalidGatewayFilterFactory();
    }

}
