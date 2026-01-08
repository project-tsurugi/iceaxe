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
package com.tsurugidb.iceaxe.test.delete;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmRetryOverIOException;

/**
 * delete (table without primary key) test
 */
class DbDeleteNoPkTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTable();
        insertTestTable(DbDeleteTest.SIZE);

        logInitEnd(info);
    }

    private static void createTable() throws Exception {
        // no primary key
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void deleteAll() throws Exception {
        new DbDeleteTest().deleteAll();
    }

    @Test
    void deleteConstant() throws Exception {
        new DbDeleteTest().deleteConstant();
    }

    @Test
    void deleteByBind() throws Exception {
        new DbDeleteTest().deleteByBind();
    }

    @RepeatedTest(15)
    void delete2SeqTx() throws Exception {
        try {
            new DbDeleteTest().delete2SeqTx();
        } catch (TsurugiTmRetryOverIOException e) {
            // TODO ERR_PHANTOMが解消したら、catchを削除する
            var c = e.getCause();
            if (c.getMessage().contains("ERR_PHANTOM")) {
                LOG.warn("delete2SeqTx fail. {}", c.getMessage());
                return;
            }
            throw e;
        }
    }

    @Test
    void delete2SameTx() throws Exception {
        new DbDeleteTest().delete2SameTx();
    }

    @Test
    void delete2Range() throws Exception {
        new DbDeleteTest().delete2Range();
    }

    @Test
    void deleteInsert() throws Exception {
        new DbDeleteTest().deleteInsert();
    }

    @Test
    void deleteInsertDeleteExists() throws Exception {
        new DbDeleteTest().deleteInsertDeleteExists();
    }

    @Test
    void deleteInsertDeleteNotExists() throws Exception {
        new DbDeleteTest().deleteInsertDeleteNotExists();
    }

    @Test
    void insertDelete() throws Exception {
        new DbDeleteTest().insertDelete();
    }

    @Test
    void insertDeleteInsert() throws Exception {
        new DbDeleteTest().insertDeleteInsert();
    }
}
