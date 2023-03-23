package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table test
 */
class DbCreateTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";

    @Test
    void create() throws IOException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.executeDdl(SQL);
    }

    @Test
    void createExists() throws IOException {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
            tm.executeDdl(SQL);
        });
        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
        assertContains("duplicate_table table `test' is already defined.", e.getMessage());
    }

    @Test
    void rollback() throws IOException {
        var session = getSession();
        assertTrue(session.findTableMetadata(TEST).isEmpty());

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(SQL)) {
            tm.execute(transaction -> {
                transaction.executeAndGetCount(ps);
                assertTrue(session.findTableMetadata(TEST).isPresent());
                transaction.rollback();
                assertTrue(session.findTableMetadata(TEST).isPresent());
            });
        }

        assertTrue(session.findTableMetadata(TEST).isPresent());
    }

    @Test
    void repeatOccWithPk() throws IOException {
        repeat(TgTxOption.ofOCC(), true);
    }

    @Test
    @Disabled // TODO remove Disabled たまにシリアライゼーションエラーが発生する
    void repeatOccNoPk() throws IOException {
        repeat(TgTxOption.ofOCC(), false);
    }

    @Test
    void repeatLtxWithPk() throws IOException {
        repeat(TgTxOption.ofLTX(), true);
    }

    @Test
    @Disabled // TODO remove Disabled tateyama-serverがクラッシュする
    void repeatLtxNoPk() throws IOException {
        repeat(TgTxOption.ofLTX(), false);
    }

    private void repeat(TgTxOption txOption, boolean hasPk) throws IOException {
        var createDdl = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + (hasPk ? ", primary key(foo)" : "") //
                + ")";
        var dropDdl = "drop table " + TEST;

        var session = getSession();
        var tm = session.createTransactionManager(txOption);
        for (int i = 0; i < 100; i++) {
            if (session.findTableMetadata(TEST).isPresent()) {
                tm.executeDdl(dropDdl);
            }

            tm.executeDdl(createDdl);
        }
    }
}
