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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
import com.tsurugidb.iceaxe.exception.IceaxeIOException;
import com.tsurugidb.iceaxe.exception.IceaxeTimeoutIOException;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TsurugiQueryResult} nextRow timeout test
 */
@Disabled // TODO remove Disabled. nextRow timeout
public class DbTimeoutRsNextRowTest extends DbTimetoutTest {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTimeoutRsNextRowTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(20_000);

        logInitEnd(LOG, info);
    }

    @Test
    void timeoutDefault() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.RS_FETCH, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSet() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyQueryResult(TsurugiQueryResult<?> result) {
                result.setFetchTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutResultRecord() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifyResultRecord(TsurugiResultRecord record) {
                record.setFetchTimeout(1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            var resultMapping = TgResultMapping.of(record -> record);
            try (var ps = session.createQuery(SELECT_SQL, resultMapping)) {
                try (var result = ps.execute(transaction)) {
                    modifier.modifyQueryResult(result);
                    result.getLowResultSet();
                    pipeServer.setPipeWrite(false);

                    try {
                        result.whileEach(record -> {
                            modifier.modifyResultRecord(record);
                        });
                    } catch (IceaxeIOException e) {
                        assertEqualsCode(IceaxeErrorCode.RS_NEXT_ROW_TIMEOUT, e);
                        return;
                    } finally {
                        pipeServer.setPipeWrite(true);
                        result.setCloseTimeout(1, TimeUnit.SECONDS);
                    }
                    fail("didn't time out");
                } catch (IceaxeTimeoutIOException e) {
                    if (e.getDiagnosticCode() != IceaxeErrorCode.RS_CLOSE_TIMEOUT) {
                        throw e;
                    }
                }
            }
        }
    }
}
