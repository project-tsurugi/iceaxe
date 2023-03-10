package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;

/**
 * generated rowid test
 *
 * <ul>
 * <li>プライマリキーが無いテーブルでは、暗黙に「__generated_rowid___テーブル名」というカラムが作られるが、このカラムはユーザーからは見えない。</li>
 * </ul>
 */
class DbGeneratedRowidTest extends DbTestTableTester {

    private static final String GENERATED_KEY = "__generated_rowid___" + TEST;
    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static void createTable() throws IOException {
        // no primary key
        var sql = "create table " + TEST //
                + "(" //
                + "  foo int," //
                + "  bar bigint," //
                + "  zzz varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @Test
    void metadata() throws IOException {
        var session = getSession();
        var metadata = session.findTableMetadata(TEST).get();
        var actualSet = metadata.getLowColumnList().stream().map(c -> c.getName()).collect(Collectors.toSet());
        var expectedSet = Set.of("foo", "bar", "zzz");
        assertEquals(expectedSet, actualSet);
    }

    @Test
    void explain() throws IOException {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of())) {
            var result = ps.explain(TgParameterList.of());
            var actualSet = result.getLowColumnList().stream().map(c -> c.getName()).collect(Collectors.toSet());
            var expectedSet = Set.of("foo", "bar", "zzz"); // 「select *」ではgenerated_rowidは出てこない
            assertEquals(expectedSet, actualSet);
        }
    }

    @Test
    void selectKey() throws IOException {
        var sql = "select " + GENERATED_KEY + " from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: error in db_->create_executable()", e.getMessage());
//TODO      assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectKeyAs() throws IOException {
        var sql = "select " + GENERATED_KEY + " as k from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: error in db_->create_executable()", e.getMessage());
//TODO      assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectKeyExpression() throws IOException {
        var sql = "select " + GENERATED_KEY + "+0 from " + TEST + " order by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: error in db_->create_executable()", e.getMessage());
//TODO      assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectGroupBy0() throws IOException {
        var sql = "select " + GENERATED_KEY + "+0, count(*) as c from " + TEST + " group by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: error in db_->create_executable()", e.getMessage());
//TODO      assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectGroupBy() throws IOException {
        var sql = "select " + GENERATED_KEY + ", count(*) as c from " + TEST + " group by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetList(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: error in db_->create_executable()", e.getMessage());
//TODO      assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void update() throws IOException {
        int key = 2;
        var sql = "update " + TEST + " set bar=11 where " + GENERATED_KEY + "=" + key;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetCount(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateKey() throws IOException {
        int key = 2;
        var sql = "update " + TEST + " set " + GENERATED_KEY + "=1, bar=11 where " + GENERATED_KEY + "=" + key;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            var e = assertThrowsExactly(TsurugiTransactionIOException.class, () -> {
                tm.executeAndGetCount(ps);
            });
            assertContains("ERR_COMPILER_ERROR: SQL--0005: translating statement failed: variable_not_found " + GENERATED_KEY, e.getMessage());
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void selectAsGeneratedRowid() throws IOException {
        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        // TODO アンダースコア2個で始まるカラム名はシステム予約でありユーザーが使用できないので、使ったらエラーになるべき
        var sql = "select foo as " + GENERATED_KEY + " from " + TEST + " order by foo";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var list = tm.executeAndGetList(ps);
            assertEquals(SIZE, list.size());
            for (var entity : list) {
                // 現状、generated_rowidで始まる別名は取得できない
                var e = assertThrowsExactly(IllegalArgumentException.class, () -> {
                    entity.getInt4(GENERATED_KEY);
                });
                assertEquals("not found column. name=__generated_rowid___test", e.getMessage());
            }
        }
    }
}
