package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * irregular select test
 */
class DbSelectIrregularTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectIrregularTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void closePsBeforeCloseRs() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createPreparedQuery(sql);
            var rs = ps.execute(transaction);
            ps.close();
            var list = rs.getRecordList();
            assertEquals(SIZE, list.size());
            rs.close();
        });
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
            assertEqualsCode(SqlServiceCode.ERR_TRANSLATOR_ERROR, e);
            // TODO エラー詳細情報の確認
        }
    }
}
