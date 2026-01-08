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

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TransactionManager execute ddl test
 */
class DbManagerExecuteDdlTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void executeDdl0() throws Exception {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = session.createTransactionManager();
        tm.executeDdl(CREATE_TEST_SQL);

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }

    @Test
    void executeDdlDefaultSetting() throws Exception {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = session.createTransactionManager(TgTxOption.ofLTX());
        tm.executeDdl(CREATE_TEST_SQL);

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }

    @Test
    void executeDdl() throws Exception {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofLTX());
        tm.executeDdl(setting, CREATE_TEST_SQL);

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }
}
