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
package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * 3 transaction test
 */
class DbTransaction3Test extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        logInitEnd(info);
    }

    @Test
    void updateOcc() throws Exception {
        update(TgTxOption.ofOCC());
    }

    @Test
    void updateLtx() throws Exception {
        update(TgTxOption.ofLTX(TEST));
    }

    private void update(TgTxOption txOption) throws Exception {
        var updateSql = "update " + TEST + " set bar=:bar where foo=0";
        var updateMapping = TgParameterMapping.ofSingle("bar", long.class);

        var session = getSession();
        try (var updatePs = session.createStatement(updateSql, updateMapping)) {
            for (int i = 0; i < 3; i++) {
                try (var t1 = session.createTransaction(txOption)) {
                    t1.getLowTransaction();
                    try (var t2 = session.createTransaction(txOption)) {
                        t2.getLowTransaction();
                        try (var t3 = session.createTransaction(txOption)) {
                            t3.getLowTransaction();

                            t1.executeAndGetCount(updatePs, 100L + i);
                            t2.executeAndGetCount(updatePs, 200L + i);
                            t3.executeAndGetCount(updatePs, 300L + i);

                            t1.commit(TgCommitType.DEFAULT);
                            var e2 = assertThrows(TsurugiTransactionException.class, () -> t2.commit(TgCommitType.DEFAULT));
                            assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e2);
                            var e3 = assertThrows(TsurugiTransactionException.class, () -> t3.commit(TgCommitType.DEFAULT));
                            assertEqualsCode(SqlServiceCode.CC_EXCEPTION, e3);
                        }
                    }
                }
            }
        }
    }
}
