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
package com.tsurugidb.iceaxe.sql.result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
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
import com.tsurugidb.iceaxe.test.low.TestResultSet;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlCommon.Column;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.ResultSetMetadata;

class TsurugiQueryResultFetchTimeoutTest {

    @Test
    void fetch_default() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);

        testFetchTimeout(sessionOption, null);
    }

    @Test
    void fetch_specified() throws Exception {
        var sessionOption = TgSessionOption.of();
        sessionOption.setTimeout(TgTimeoutKey.RS_FETCH, 1, TimeUnit.SECONDS);

        testFetchTimeout(sessionOption, null);
    }

    @Test
    void fetch_set() throws Exception {
        var sessionOption = TgSessionOption.of();

        testFetchTimeout(sessionOption, ps -> ps.setFetchTimeout(1, TimeUnit.SECONDS));
    }

    private void testFetchTimeout(TgSessionOption sessionOption, Consumer<TsurugiQueryResult<?>> modifier) throws Exception {
        var future = new TestFutureResponse<ResultSet>() {
            @Override
            protected ResultSet getInternal() {
                return new TestResultSet() {
                    @Override
                    public boolean nextRow() throws IOException, ServerException, InterruptedException {
                        return true;
                    }

                    @Override
                    public ResultSetMetadata getMetadata() throws IOException, ServerException, InterruptedException {
                        return new ResultSetMetadata() {
                            @Override
                            public List<? extends Column> getColumns() {
                                var c = Column.newBuilder().setName("test").setAtomType(AtomType.INT4).build();
                                return List.of(c);
                            }
                        };
                    }

                    @Override
                    public boolean nextColumn() throws IOException, ServerException, InterruptedException {
                        return true;
                    }

                    @Override
                    public boolean isNull() {
                        return false;
                    }

                    @Override
                    public int fetchInt4Value() throws IOException, ServerException, InterruptedException {
                        assertEquals(1, timeout.value());
                        assertEquals(TimeUnit.SECONDS, timeout.unit());
                        throw new ResponseTimeoutException("test");
                    }
                };
            }
        };

        try (var session = new TestTsurugiSession(sessionOption)) {
            try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
                var lowTx = (TestLowTransaction) transaction.getLowTransaction();
                lowTx.setTestResultSetFutureResponse(future);

                try (var ps = session.createQuery("SQL")) {
                    var rs = ps.execute(transaction);
                    if (modifier != null) {
                        modifier.accept(rs);
                    }

                    var e = assertThrowsExactly(IceaxeTimeoutIOException.class, () -> rs.getRecordList());
                    assertEquals(IceaxeErrorCode.RS_FETCH_TIMEOUT, e.getDiagnosticCode());
                }
            }
        }

        assertTrue(future.isClosed());
    }
}
