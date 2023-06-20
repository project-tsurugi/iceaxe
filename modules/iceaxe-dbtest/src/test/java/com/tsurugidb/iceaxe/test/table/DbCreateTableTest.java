package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table test
 */
class DbCreateTableTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    private static final String SQL = "create table " + TEST //
            + "(" //
            + "  foo int," //
            + "  bar bigint," //
            + "  zzz varchar(10)," //
            + "  primary key(foo)" //
            + ")";

    @Test
    void create() throws Exception {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.executeDdl(SQL);
    }

    @Test
    void createExists() throws Exception {
        createTestTable();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeDdl(SQL);
        });
        assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
        assertContains("duplicate_table table `test' is already defined.", e.getMessage());
    }

    @Test
    void alreadyExists() throws Exception {
        var session = getSession();
        var txOption = TgTxOption.ofDDL();

        // preparedStatementを作る際にテーブルが存在していないので、ERR_COMPILER_ERRORにならない
        try (var ps = session.createStatement(CREATE_TEST_SQL, TgParameterMapping.of())) {
            Thread.sleep(100); // preparedStatement作成がDBサーバー側で処理されるのを待つ
            // テーブルを作成する
            createTestTable();

            // テーブルが作られた後にpreparedStatementのDDLを実行するとERR_ALREADY_EXISTSになる
            try (var transaction = session.createTransaction(txOption)) {
                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> transaction.executeAndGetCount(ps, TgBindParameters.of()));
                assertEqualsCode(SqlServiceCode.ERR_ALREADY_EXISTS, e);
                transaction.rollback();
            }
        }

        try (var ps = session.createStatement(CREATE_TEST_SQL, TgParameterMapping.of())) {
            try (var transaction = session.createTransaction(txOption)) {
                var e = assertThrowsExactly(TsurugiIOException.class, () -> transaction.executeAndGetCount(ps, TgBindParameters.of()));
                assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
                assertContains("duplicate_table table `test' is already defined", e.getMessage());
                transaction.rollback();
            }
        }
    }

    @Test
    void rollback() throws Exception {
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
    void repeatOccWithPk() throws Exception {
        repeat(TgTxOption.ofOCC(), true);
    }

    @Test
    @Disabled // TODO remove Disabled たまにシリアライゼーションエラーが発生する
    void repeatOccNoPk() throws Exception {
        repeat(TgTxOption.ofOCC(), false);
    }

    @Test
    void repeatLtxWithPk() throws Exception {
        repeat(TgTxOption.ofLTX(), true);
    }

    @Test
    @Disabled // TODO remove Disabled tateyama-serverがクラッシュする
    void repeatLtxNoPk() throws Exception {
        repeat(TgTxOption.ofLTX(), false);
    }

    private void repeat(TgTxOption txOption, boolean hasPk) throws IOException, InterruptedException {
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
