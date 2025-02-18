/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.tsubakuro.sql.TableList;

class TsurugiTableListHelperTimeoutTest {

    @Test
    void connectTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_LIST_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableList>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableListFutureResponse(future);

            var target = new TsurugiTableListHelper();

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTableList(session));
            assertEquals(IceaxeErrorCode.TABLE_LIST_CONNECT_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }

    @Test
    void closeTimeout() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TABLE_LIST_CONNECT, 1, TimeUnit.SECONDS);

        var future = new TestFutureResponse<TableList>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTableListFutureResponse(future);

            var target = new TsurugiTableListHelper();

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> target.getTableList(session));
            assertEquals(IceaxeErrorCode.TABLE_LIST_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
    }
}
