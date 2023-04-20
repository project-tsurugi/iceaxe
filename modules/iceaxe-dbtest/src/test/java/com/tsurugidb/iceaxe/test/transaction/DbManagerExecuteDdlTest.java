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
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
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
