package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * update test
 */
class DbUpdateTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void updateAll() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertEquals(0L, entity.getBar());
            assertEquals("aaa", entity.getZzz());
        }
    }

    @Test
    void updateAllNull() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertNull(entity.getBar());
            assertNull(entity.getZzz());
        }
    }

    @Test
    void updateWhere() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo % 2 = 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            if (entity.getFoo() % 2 == 0) {
                assertEquals(0L, entity.getBar());
                assertEquals("aaa", entity.getZzz());
            } else {
                assertEquals((long) i, entity.getBar());
                assertEquals(Integer.toString(i), entity.getZzz());
            }
            i++;
        }
    }

    @Test
    void updateNothing() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo < 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO 0
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateEntity() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = :bar," //
                + "  zzz = :zzz" //
                + " where foo = :foo";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .int4("foo", TestEntity::getFoo) //
                .int8("bar", TestEntity::getBar) //
                .character("zzz", TestEntity::getZzz);

        var updateEntity = new TestEntity(5, 55, "go");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            int count = ps.executeAndGetCount(tm, updateEntity);
            assertEquals(-1, count); // TODO 1
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            if (entity.getFoo().equals(updateEntity.getFoo())) {
                assertEquals(updateEntity, entity);
            } else {
                var expected = createTestEntity(i);
                assertEquals(expected, entity);
            }
            i++;
        }
    }

    @Test
    void updateExpression() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = bar + 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            var expected = new TestEntity(i, i + 1, Integer.toString(i));
            assertEquals(expected, entity);
            i++;
        }
    }

    @Test
    void updatePK() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo + 1"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            var expected = new TestEntity(i + 1, i, Integer.toString(i));
            assertEquals(expected, entity);
            i++;
        }
    }

    @Test
    void updatePKNoChange() throws IOException {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedStatement(sql)) {
            int count = ps.executeAndGetCount(tm);
            assertEquals(-1, count); // TODO SIZE
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertUpdate() throws IOException {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // insert
            try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                int count = ps.executeAndGetCount(tranasction, insertEntity);
                assertEquals(-1, count); // TODO 1
            }

            // update
            try (var ps = session.createPreparedStatement(sql)) {
                int count = ps.executeAndGetCount(tranasction);
                assertEquals(-1, count); // TODO 1
            }

            // select
            try (var ps = session.createPreparedQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = ps.executeAndGetList(tranasction);
                assertEquals(SIZE + 1, list.size());
                for (var entity : list) {
                    if (entity.getFoo().equals(insertEntity.getFoo())) {
                        assertEquals(789L, entity.getBar());
                    } else {
                        assertEquals((long) entity.getFoo(), entity.getBar());
                    }
                }
            }
        });

        var list = selectAllFromTest();
        assertEquals(SIZE + 1, list.size());
        for (var entity : list) {
            if (entity.getFoo().equals(insertEntity.getFoo())) {
                assertEquals(789L, entity.getBar());
            } else {
                assertEquals((long) entity.getFoo(), entity.getBar());
            }
        }
    }

    @Test
    void insertUpdateNoCheck() throws IOException {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // insert
            try (var ps = session.createPreparedStatement(INSERT_SQL, INSERT_MAPPING)) {
                ps.execute(tranasction, insertEntity); // not get-count
            }
            // executeの結果を確認せずに次のSQLを実行すると、同一トランザクション内でもSQLの実行順序が保証されないらしい

            // update
            try (var ps = session.createPreparedStatement(sql)) {
                ps.execute(tranasction); // not get-count
            }

            // select
            try (var ps = session.createPreparedQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = ps.executeAndGetList(tranasction);
                assertEquals(SIZE + 1, list.size());
                for (var entity : list) {
                    if (entity.getFoo().equals(insertEntity.getFoo())) {
                        assertEquals(789L, entity.getBar());
                    } else {
                        assertEquals((long) entity.getFoo(), entity.getBar());
                    }
                }
            }
        });

        var list = selectAllFromTest();
        assertEquals(SIZE + 1, list.size());
        for (var entity : list) {
            if (entity.getFoo().equals(insertEntity.getFoo())) {
                assertEquals(789L, entity.getBar());
            } else {
                assertEquals((long) entity.getFoo(), entity.getBar());
            }
        }
    }

    @Test
    void updateUpdate() throws IOException {
        int foo = 1;
        var bar = TgVariable.ofInt8("bar");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = " + bar //
                + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // update
            try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
                {
                    var param = TgParameterList.of(bar.bind(101));
                    int count = ps.executeAndGetCount(tranasction, param);
                    assertEquals(-1, count); // TODO 1
                }
                {
                    var param = TgParameterList.of(bar.bind(102));
                    int count = ps.executeAndGetCount(tranasction, param);
                    assertEquals(-1, count); // TODO 1
                }
            }

            // select
            try (var ps = session.createPreparedQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = ps.executeAndGetList(tranasction);
                assertEquals(SIZE, list.size());
                for (var entity : list) {
                    if (entity.getFoo().equals(foo)) {
                        assertEquals(102L, entity.getBar());
                    } else {
                        assertEquals((long) entity.getFoo(), entity.getBar());
                    }
                }
            }
        });

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            if (entity.getFoo().equals(foo)) {
                assertEquals(102L, entity.getBar());
            } else {
                assertEquals((long) entity.getFoo(), entity.getBar());
            }
        }
    }
}
