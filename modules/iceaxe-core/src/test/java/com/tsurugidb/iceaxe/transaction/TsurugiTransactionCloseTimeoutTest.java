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
package com.tsurugidb.iceaxe.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.test.TestTsurugiSession;
import com.tsurugidb.iceaxe.test.low.TestFutureResponse;
import com.tsurugidb.iceaxe.test.low.TestLowTransaction;
import com.tsurugidb.iceaxe.test.low.TestSqlClient;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;
import com.tsurugidb.tsubakuro.sql.Transaction;

class TsurugiTransactionCloseTimeoutTest {

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_CLOSE, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, tx -> tx.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        var future = new TestFutureResponse<Transaction>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var client = (TestSqlClient) session.getLowSqlClient();
            client.setTestTransactionFutureResponse(future);

            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (modifier != null) {
                modifier.accept(transaction);
            }

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.close());
            assertEquals(IceaxeErrorCode.TX_CLOSE_TIMEOUT, e.getDiagnosticCode());

            future.setExpectedCloseTimeout(null);
            future.setThrowCloseTimeout(false);
        }

        assertTrue(future.isClosed());
    }

    @Test
    void lowCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_CLOSE, 1, TimeUnit.SECONDS);

        testLowCloseTimeout(sessionOption, null);
    }

    @Test
    void lowCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testLowCloseTimeout(sessionOption, tx -> tx.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testLowCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        try (var session = new TestTsurugiSession(sessionOption)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (modifier != null) {
                modifier.accept(transaction);
            }

            var lowTx = (TestLowTransaction) transaction.getLowTransaction();
            lowTx.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
            lowTx.setThrowCloseTimeout(true);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.close());
            assertEquals(IceaxeErrorCode.TX_CLOSE_TIMEOUT, e.getDiagnosticCode());

            assertTrue(lowTx.isClosed());

            lowTx.setExpectedCloseTimeout(null);
            lowTx.setThrowCloseTimeout(false);
        }
    }

    @Test
    void closeTimeoutWithChild_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testCloseTimeoutWithChild(sessionOption, null);
    }

    @Test
    void closeTimeoutWithChild_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_CLOSE, 1, TimeUnit.SECONDS);

        testCloseTimeoutWithChild(sessionOption, null);
    }

    @Test
    void closeTimeoutWithChild_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testCloseTimeoutWithChild(sessionOption, tx -> tx.setCloseTimeout(1, TimeUnit.SECONDS));
    }

    private void testCloseTimeoutWithChild(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        long childSleep = TimeUnit.MILLISECONDS.toNanos(50);
        var child = new IceaxeTimeoutCloseable() {

            boolean closed = false;

            @Override
            public void close(long timeoutNanos) throws Exception {
                this.closed = true;
                TimeUnit.NANOSECONDS.sleep(childSleep * 2);
            }
        };

        try (var session = new TestTsurugiSession(sessionOption)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (modifier != null) {
                modifier.accept(transaction);
            }

            transaction.addChild(child);

            var lowTx = (TestLowTransaction) transaction.getLowTransaction();
            lowTx.setExpectedCloseTimeout(TimeUnit.SECONDS.toNanos(1) - childSleep, TimeUnit.NANOSECONDS);
            lowTx.setThrowCloseTimeout(true);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.close());
            assertEquals(IceaxeErrorCode.TX_CLOSE_TIMEOUT, e.getDiagnosticCode());

            assertTrue(lowTx.isClosed());

            lowTx.setExpectedCloseTimeout(null);
            lowTx.setThrowCloseTimeout(false);
        }

        assertTrue(child.closed);
    }
}
