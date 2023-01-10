package com.tsurugidb.iceaxe.test.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;

/**
 * generated rowid test
 */
// プライマリキーが無いテーブルでは暗黙にgenerated_rowidカラムが作られる
//TODO generated_rowidが見えてはいけない
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
        // TODO TableMetadataでgenerated_rowidは取得できないべき
        var expectedSet = Set.of("foo", "bar", "zzz", GENERATED_KEY);
        assertEquals(expectedSet, actualSet);

        var key = metadata.getLowColumnList().stream().filter(c -> c.getName().equals(GENERATED_KEY)).findAny().get();
        assertEquals(AtomType.INT8, key.getAtomType());
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
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            for (var entity : list) {
                assertEquals(List.of(), entity.getNameList());
            }
        }
    }

    @Test
    void selectKeyAs() throws IOException {
        var sql = "select " + GENERATED_KEY + " as k from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            int i = 0;
            for (var entity : list) {
                assertEquals(++i, entity.getInt8("k"));
            }
        }
    }

    @Test
    void selectKeyExpression() throws IOException {
        var sql = "select " + GENERATED_KEY + "+0 from " + TEST + " order by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            int i = 0;
            for (var entity : list) {
                assertEquals(++i, entity.getInt8("@#0"));
            }
        }
    }

    @Test
    void selectGroupBy0() throws IOException {
        var sql = "select " + GENERATED_KEY + "+0, count(*) as c from " + TEST + " group by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            for (var entity : list) {
                assertEquals(1, entity.getInt4("c"));
            }
        }
    }

    @Test
//    @RepeatedTest(200) // 大量に繰り返すと、稀にfailする
    void selectGroupBy() throws IOException {
        var sql = "select " + GENERATED_KEY + ", count(*) as c from " + TEST + " group by " + GENERATED_KEY;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
            int i = 0;
            for (var entity : list) {
                // TODO cで件数が取得されるべき
//              assertEquals(1, entity.getInt4("c"));
                try {
                    assertEquals(++i, entity.getInt8("c"));
                } catch (Throwable t) {
                    LOG.warn("selectGroupBy.list={}", list);
                    throw t;
                }
            }
        }
    }

    @Test
    void update() throws IOException {
        int key = 2;
        var sql = "update " + TEST + " set bar=11 where " + GENERATED_KEY + "=" + key;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO 1
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            if (entity.getFoo() == key - 1) {
                assertEquals(11L, entity.getBar());
            } else {
                assertEquals(entity.getFoo().longValue(), entity.getBar());
            }
        }
    }

    @Test
    void updateKey() throws IOException {
        int key = 2;
        var sql = "update " + TEST + " set " + GENERATED_KEY + "=1, bar=11 where " + GENERATED_KEY + "=" + key;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        // TODO generated_rowidが（見えなくて）エラーになるべき
        try (var ps = session.createPreparedStatement(sql)) {
            // generated_rowidが使えるならば、duplicate keyが発生すべき
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE - 1, list.size());
        for (var entity : list) {
            if (entity.getFoo() == key - 1) {
                assertEquals(11L, entity.getBar());
            } else {
                assertEquals(entity.getFoo().longValue(), entity.getBar());
            }
        }
    }

    @Test
    void selectAsGeneratedRowid() throws IOException {
        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        var sql = "select foo as " + GENERATED_KEY + " from " + TEST + " order by foo";
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql)) {
            var list = ps.executeAndGetList(tm);
            assertEquals(SIZE, list.size());
//          int i = 0;
            for (var entity : list) {
//TODO          assertEquals(i++, entity.getInt4(GENERATED_KEY));
                // 現状、generated_rowidで始まるカラムは取得できない
                var e = assertThrowsExactly(IllegalArgumentException.class, () -> {
                    entity.getInt4(GENERATED_KEY);
                });
                assertEquals("not found column. name=__generated_rowid___test", e.getMessage());
            }
        }
    }
}
