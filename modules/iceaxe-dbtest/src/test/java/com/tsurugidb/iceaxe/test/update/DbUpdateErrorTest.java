package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * update error test
 */
class DbUpdateErrorTest extends DbTestTableTester {

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
    void updatePKNull() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = null" // primary key
                + " where foo = 5";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            // TODO updatePK null
//          var e = assertThrowsExactly(TsurugiIOException.class, () -> tm.executeAndGetCount(ps));
//          assertEqualsCode(null, e); // TODO エラーコード
            tm.executeAndGetCount(ps);
        }

//      assertEqualsTestTable(SIZE);
        var actual = selectAllFromTest();
        for (var entity : actual) {
            if (entity.getBar() == 5) {
                assertNull(entity.getFoo());
            } else {
                assertEquals((int) (long) entity.getBar(), entity.getFoo());
            }
        }
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
            // TODO updatePK same value
//          var e = assertThrowsExactly(TsurugiIOException.class, () -> tm.executeAndGetCount(ps));
//          assertEqualsCode(null, e); // TODO エラーコード
            tm.executeAndGetCount(ps);
        }

//      assertEqualsTestTable(SIZE);
        var actual = selectAllFromTest();
        assertEquals(1, actual.size());
        for (var entity : actual) {
            assertEquals(newPk, entity.getFoo());
        }
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
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetCount(ps);
            });
            assertEqualsCode(SqlServiceCode.ERR_INTEGRITY_CONSTRAINT_VIOLATION, e);
            String expected = "ERR_INTEGRITY_CONSTRAINT_VIOLATION: SQL--0016: . attempt=0, option=OCC{}";
            assertContains(expected, e.getMessage()); // TODO エラー詳細情報の確認
        }

        assertEqualsTestTable(SIZE);
    }
}
