package com.tsurugidb.iceaxe.test.transaction;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * TransactionManager execute statement test
 */
class DbManagerExecuteStatementTest extends DbTestTableTester {

    private static final int SIZE = 2;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL;
    static {
        var entity = createTestEntity(SIZE);
        SQL = "insert into " + TEST //
                + " (" + TEST_COLUMNS + ")" //
                + " values(" + entity.getFoo() + "," + entity.getBar() + ",'" + entity.getZzz() + "')";
    }

    @Test
    void executeAndGetCount_sql() throws IOException, TsurugiTransactionException {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(SQL);

        assertEquals(-1, result); // TODO 1
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_sql() throws IOException, TsurugiTransactionException {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(setting, SQL);

        assertEquals(-1, result); // TODO 1
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_sql_parameter() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(INSERT_SQL, INSERT_MAPPING, entity);

        assertEquals(-1, result); // TODO 1
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_sql_parameter() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        int result = tm.executeAndGetCount(setting, INSERT_SQL, INSERT_MAPPING, entity);

        assertEquals(-1, result); // TODO 1
        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_ps() throws IOException, TsurugiTransactionException {
        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedStatement(SQL)) {
            int result = tm.executeAndGetCount(ps);

            assertEquals(-1, result); // TODO 1
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_ps() throws IOException, TsurugiTransactionException {
        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedStatement(SQL)) {
            int result = tm.executeAndGetCount(setting, ps);

            assertEquals(-1, result); // TODO 1
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_ps_parameter() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager(TgTxOption.ofOCC());

        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            int result = tm.executeAndGetCount(ps, entity);

            assertEquals(-1, result); // TODO 1
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void executeAndGetCount_setting_ps_parameter() throws IOException, TsurugiTransactionException {
        var entity = createTestEntity(SIZE);

        var session = getSession();
        var tm = session.createTransactionManager();
        var setting = TgTmSetting.of(TgTxOption.ofOCC());

        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            int result = tm.executeAndGetCount(setting, ps, entity);

            assertEquals(-1, result); // TODO 1
        }

        assertEqualsTestTable(SIZE + 1);
    }
}