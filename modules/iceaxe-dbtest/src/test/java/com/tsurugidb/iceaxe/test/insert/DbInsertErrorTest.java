package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * insert error test
 */
class DbInsertErrorTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        if (!info.getDisplayName().equals("insertNullToNotNull()")) {
            createTestTable();
        }

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void insertNullToPK() throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(null, 456, 'abc')";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(0);
    }

    @Test
    void insertNullToNotNull() throws IOException {
        var session = getSession();

        var createSql = "create table " + TEST //
                + "(" //
                + "  foo int not null," //
                + "  bar bigint not null," //
                + "  zzz varchar(10) not null," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(session, createSql);

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                var entity = new TestEntity(123, null, null);
                ps.executeAndGetCount(tm, entity);
            });
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(0);
    }
}
