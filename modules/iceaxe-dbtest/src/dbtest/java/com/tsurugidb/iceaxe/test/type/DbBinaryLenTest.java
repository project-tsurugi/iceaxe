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
package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.type.DbVarbinaryLenTest.VarbinaryTester;
import com.tsurugidb.iceaxe.test.util.DbTestConnector;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * binary test
 */
class DbBinaryLenTest extends DbTestTableTester {

    private static final int MAX_LENGTH = 2097132;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void createError() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session, "executeDdl", 1);

        var createSql = getCreateTable(MAX_LENGTH + 1);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeDdl(createSql);
        });
        assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
        assertContains("octet type on column \"value\" is unsupported (invalid length)", e.getMessage());
    }

    private static String getCreateTable(int length) {
        return "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value binary(" + length + ")" //
                + ")";
    }

    @ParameterizedTest
    @ValueSource(ints = { 100, MAX_LENGTH - 1, MAX_LENGTH })
    void insertUpdate(int maxLength) throws Exception {
        if (maxLength >= MAX_LENGTH - 1) { // TODO remove assume IPC
            assumeFalse(DbTestConnector.isIpc());
        }
        new BinaryTester(maxLength).test();
    }

    private static class BinaryTester extends VarbinaryTester {

        public BinaryTester(int maxLength) {
            super(maxLength);
        }

        @Override
        protected String getCreateSql() {
            return getCreateTable(maxLength);
        }

        @Override
        protected byte[] getExpectedValue(byte[] value) {
            if (value.length == maxLength) {
                return value;
            }

            return Arrays.copyOf(value, maxLength);
        }
    }
}
