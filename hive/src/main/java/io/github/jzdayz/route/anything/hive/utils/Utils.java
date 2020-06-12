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
package io.github.jzdayz.route.anything.hive.utils;

import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.rpc.thrift.TStatus;
import org.apache.hive.service.rpc.thrift.TStatusCode;

import java.sql.SQLException;

public class Utils {

    // Verify success or success_with_info status, else throw SQLException
    public static void verifySuccessWithInfo(TStatus status) throws SQLException {
        verifySuccess(status, true);
    }

    // Verify success status, else throw SQLException
    public static void verifySuccess(TStatus status) throws SQLException {
        verifySuccess(status, false);
    }

    // Verify success and optionally with_info status, else throw SQLException
    public static void verifySuccess(TStatus status, boolean withInfo) throws SQLException {
        if (status.getStatusCode() == TStatusCode.SUCCESS_STATUS ||
                (withInfo && status.getStatusCode() == TStatusCode.SUCCESS_WITH_INFO_STATUS)) {
            return;
        }
        throw new HiveSQLException(status);
    }

}
