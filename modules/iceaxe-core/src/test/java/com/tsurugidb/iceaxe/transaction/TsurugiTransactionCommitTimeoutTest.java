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
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.util.IceaxeTimeoutCloseable;

class TsurugiTransactionCommitTimeoutTest {

    @Test
    void commitTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testCommitTimeout(sessionOption, null);
    }

    @Test
    void commitTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.SECONDS);

        testCommitTimeout(sessionOption, null);
    }

    @Test
    void commitTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testCommitTimeout(sessionOption, tx -> tx.setCommitTimeout(1, TimeUnit.SECONDS));
    }

    private void testCommitTimeout(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        var future = new TestFutureResponse<Void>();
        future.setExpectedTimeout(1, TimeUnit.SECONDS);
        future.setThrowTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                if (modifier != null) {
                    modifier.accept(transaction);
                }

                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestCommitFutureResponse(future);

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.commit(TgCommitType.DEFAULT));
                assertEquals(IceaxeErrorCode.TX_COMMIT_TIMEOUT, e.getDiagnosticCode());
            }
        }

        assertTrue(future.isClosed());
    }

    @Test
    void futureCloseTimeout_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.SECONDS);

        testFutureCloseTimeout(sessionOption, null);
    }

    @Test
    void futureCloseTimeout_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFutureCloseTimeout(sessionOption, tx -> tx.setCommitTimeout(1, TimeUnit.SECONDS));
    }

    private void testFutureCloseTimeout(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        var future = new TestFutureResponse<Void>();
        future.setExpectedCloseTimeout(1, TimeUnit.SECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                if (modifier != null) {
                    modifier.accept(transaction);
                }

                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestCommitFutureResponse(future);

                var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.commit(TgCommitType.DEFAULT));
                assertEquals(IceaxeErrorCode.TX_COMMIT_CLOSE_TIMEOUT, e.getDiagnosticCode());
            }

            future.setExpectedCloseTimeout(null);
            future.setThrowCloseTimeout(false);
        }

        assertTrue(future.isClosed());
    }

    @Test
    void commitTimeoutWithChild_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testCommitTimeoutWithChild(sessionOption, null);
    }

    @Test
    void commitTimeoutWithChild_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.TRANSACTION_COMMIT, 1, TimeUnit.SECONDS);

        testCommitTimeoutWithChild(sessionOption, null);
    }

    @Test
    void commitTimeoutWithChild_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testCommitTimeoutWithChild(sessionOption, tx -> tx.setCommitTimeout(1, TimeUnit.SECONDS));
    }

    private void testCommitTimeoutWithChild(TgSessionOption sessionOption, Consumer<TsurugiTransaction> modifier) throws Exception {
        long childSleep = TimeUnit.MILLISECONDS.toNanos(50);
        var child = new IceaxeTimeoutCloseable() {

            boolean closed = false;

            @Override
            public void close(long timeoutNanos) throws Exception {
                this.closed = true;
                TimeUnit.NANOSECONDS.sleep(childSleep * 2);
            }
        };

        var future = new TestFutureResponse<Void>();
        future.setExpectedCloseTimeout(TimeUnit.SECONDS.toNanos(1) - childSleep, TimeUnit.NANOSECONDS);
        future.setThrowCloseTimeout(true);

        try (var session = new TestTsurugiSession(sessionOption)) {
            var transaction = session.createTransaction(TgTxOption.ofOCC());
            if (modifier != null) {
                modifier.accept(transaction);
            }

            transaction.addChild(child);

            var lowTx = (TestLowTransaction) transaction.getLowTransaction();
            lowTx.setTestCommitFutureResponse(future);

            var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> transaction.commit(TgCommitType.DEFAULT));
            assertEquals(IceaxeErrorCode.TX_COMMIT_CLOSE_TIMEOUT, e.getDiagnosticCode());
        }

        assertTrue(future.isClosed());
        assertTrue(child.closed);
    }
}
