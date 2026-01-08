/*
 * Copyright 2023-2026 Project Tsurugi.
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
package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.Link;

/**
 * slot limit test
 */
public class DbSlotLimitTest extends DbTimetoutTest {

    private static final int ATTEMPT_SIZE = Link.responseBoxSize() + 100;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void slotLimit() throws Exception {
        testTimeout(new TimeoutModifier());
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        try {
            transaction.setCloseTimeout(1, TimeUnit.SECONDS); // TODO 本来はトランザクションはタイムアウトせず正常にクローズできて欲しい
            transaction.getLowTransaction();

            try (var ps = session.createQuery(SELECT_SQL)) {
                var resultList = new ArrayList<TsurugiQueryResult<?>>();

                pipeServer.setPipeWrite(false);
                try {
                    execute(transaction, ps, resultList);
                } finally {
                    pipeServer.setPipeWrite(true);
                }

                int i = 0;
                for (var result : resultList) {
                    LOG.trace("close i={}", i);
                    try {
                        result.close();
                    } catch (IceaxeIOException e) {
                        assertEqualsCode(IceaxeErrorCode.RS_CONNECT_TIMEOUT, e); // close内で接続しているため、コネクトタイムアウト
                    }
                    i++;
                }
            }
        } finally {
            try {
                transaction.close();
            } catch (IceaxeIOException e) {
                assertEqualsCode(IceaxeErrorCode.TX_CLOSE_TIMEOUT, e);
                // TODO 本来はタイムアウトせず正常にクローズできて欲しい
                LOG.warn("transaction.close() {}", e.getMessage());
            }
        }
    }

    private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<TsurugiResultEntity> ps, List<TsurugiQueryResult<?>> resultList)
            throws IOException, InterruptedException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            LOG.trace("i={}", i);
            try {
                var result = ps.execute(transaction);
                result.setConnectTimeout(1, TimeUnit.MILLISECONDS);
                result.setCloseTimeout(1, TimeUnit.MILLISECONDS);
                resultList.add(result);
            } catch (Throwable t) {
                LOG.error("excption occurred. i={}", i, t);
                throw t;
            }
        }
//      fail("slot limit over did not occur");
    }
}
