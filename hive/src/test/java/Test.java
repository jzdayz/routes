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

import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    public static void main(String[] args) throws Exception{


        HikariDataSource data = new HikariDataSource();
        data.setJdbcUrl("jdbc:hive2://localhost:20001/default");
        data.setUsername("user2");
        data.setPassword("123");

        data.setMaximumPoolSize(10);

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 1000; i++) {
            executorService.submit(()->{
                try {
                    JdbcTemplate jdbcTemplate = new JdbcTemplate(data);
                    System.out.println(jdbcTemplate.queryForMap("select * from d9"));;
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
            });
        }


    }
}
