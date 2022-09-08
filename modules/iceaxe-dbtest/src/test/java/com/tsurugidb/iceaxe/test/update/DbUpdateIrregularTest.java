package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * irregular update test
 */
class DbUpdateIrregularTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        if (!info.getDisplayName().equals("updateNullToNotNull()")) {
            createTestTable();
            insertTestTable(SIZE);
        }

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void closePsBeforeCloseRc() throws IOException {
        int number = SIZE / 2;
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo = " + number;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(transaction -> {
            var ps = session.createPreparedStatement(sql);
            var rc = ps.execute(transaction);
            ps.close();
            int count = rc.getUpdateCount();
            assertEquals(-1, count); // TODO 1
            rc.close();
        });

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            if (entity.getFoo() == number) {
                assertEquals(0L, entity.getBar());
                assertEquals("aaa", entity.getZzz());
            } else {
                int i = entity.getFoo();
                var expected = createTestEntity(i);
                assertEquals(expected, entity);
            }
        }
    }

    @Test
    void updatePKNull() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = null" // primary key
                + " where foo = 5";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(null, e); // TODO エラーコード
        }

        assertEqualsTestTable(SIZE);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, SIZE / 2, SIZE * 2 })
    void updatePKSameValue(int newPk) throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = " + newPk; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> ps.executeAndGetCount(tm));
            assertEqualsCode(null, e); // TODO エラーコード
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateNullToNotNull() throws IOException {
        var session = getSession();

        var createSql = "create table " + TEST //
                + "(" //
                + "  foo int not null," //
                + "  bar bigint not null," //
                + "  zzz varchar(10) not null," //
                + "  primary key(foo)" //
                + ")";
        executeDdl(session, createSql);
        insertTestTable(SIZE);

        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrows(TsurugiIOException.class, () -> {
                ps.executeAndGetCount(tm);
            });
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
