package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select error test
 */
class DbSelectErrorTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectErrorTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void undefinedColumnName() throws IOException {
        var sql = "select hoge from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrows(TsurugiTransactionIOException.class, () -> {
                ps.executeAndGetList(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_COMPILER_ERROR, e);
            assertTrue(e.getMessage().contains("TODO"), () -> "actual=" + e.getMessage()); // TODO エラー詳細情報の確認
        }
    }
}
