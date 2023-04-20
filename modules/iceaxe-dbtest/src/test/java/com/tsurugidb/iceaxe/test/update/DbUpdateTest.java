package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * update test
 */
class DbUpdateTest extends DbTestTableTester {

    private static final int SIZE = 10;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void updateAll() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertEquals(0L, entity.getBar());
            assertEquals("aaa", entity.getZzz());
        }
    }

    @Test
    void updateAllNull() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = null," //
                + "  zzz = null";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        for (var entity : list) {
            assertNull(entity.getBar());
            assertNull(entity.getZzz());
        }
    }

    @Test
    void updateWhere() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo % 2 = 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            if (entity.getFoo() % 2 == 0) {
                assertEquals(0L, entity.getBar());
                assertEquals("aaa", entity.getZzz());
            } else {
                assertEquals(i, entity.getBar());
                assertEquals(Integer.toString(i), entity.getZzz());
            }
            i++;
        }
    }

    @Test
    void updateNothing() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 0," //
                + "  zzz = 'aaa'" //
                + " where foo < 0";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(0, count);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void updateEntity() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = :bar," //
                + "  zzz = :zzz" //
                + " where foo = :foo";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt("foo", TestEntity::getFoo) //
                .addLong("bar", TestEntity::getBar) //
                .addString("zzz", TestEntity::getZzz);

        var updateEntity = new TestEntity(5, 55, "go");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            int count = tm.executeAndGetCount(ps, updateEntity);
            assertUpdateCount(1, count);
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
    void updateExpression() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  bar = bar + 1";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
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
    void updatePK() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo + 1"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        var list = selectAllFromTest();
        assertEquals(SIZE, list.size());
        int i = 0;
        for (var entity : list) {
            // TODO updatePK
//          var expected = new TestEntity(i + 1, i, Integer.toString(i));
            var expected = (i + 1 != 10) ? new TestEntity(i + 1, i + 1, Integer.toString(i + 1)) : new TestEntity(10, 0, Integer.toString(0));
            assertEquals(expected, entity);
            i++;
        }
    }

    @Test
    void updatePKNoChange() throws Exception {
        var sql = "update " + TEST //
                + " set" //
                + "  foo = foo"; // primary key

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql)) {
            int count = tm.executeAndGetCount(ps);
            assertUpdateCount(SIZE, count);
        }

        assertEqualsTestTable(SIZE);
    }

    @Test
    void insertUpdate() throws Exception {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // insert
            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                int count = tranasction.executeAndGetCount(ps, insertEntity);
                assertUpdateCount(1, count);
            }

            // update
            try (var ps = session.createStatement(sql)) {
                int count = tranasction.executeAndGetCount(ps);
                assertUpdateCount(1, count);
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
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
    void insertUpdateNoCheck() throws Exception {
        var insertEntity = new TestEntity(123, 456, "abc");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = 789" //
                + " where foo = " + insertEntity.getFoo();

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // insert
            try (var ps = session.createStatement(INSERT_SQL, INSERT_MAPPING)) {
                ps.execute(tranasction, insertEntity); // not get-count
            }
            // executeの結果を確認せずに次のSQLを実行すると、同一トランザクション内でもSQLの実行順序が保証されないらしい

            // update
            try (var ps = session.createStatement(sql)) {
                ps.execute(tranasction); // not get-count
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
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
    void updateUpdate() throws Exception {
        int foo = 1;
        var bar = TgBindVariable.ofLong("bar");
        var sql = "update " + TEST //
                + " set" //
                + "  bar = " + bar //
                + " where foo = " + foo;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        tm.execute(tranasction -> {
            // update
            try (var ps = session.createStatement(sql, parameterMapping)) {
                {
                    var parameter = TgBindParameters.of(bar.bind(101));
                    int count = tranasction.executeAndGetCount(ps, parameter);
                    assertUpdateCount(1, count);
                }
                {
                    var parameter = TgBindParameters.of(bar.bind(102));
                    int count = tranasction.executeAndGetCount(ps, parameter);
                    assertUpdateCount(1, count);
                }
            }

            // select
            try (var ps = session.createQuery(SELECT_SQL, SELECT_MAPPING)) {
                var list = tranasction.executeAndGetList(ps);
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
