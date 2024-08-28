package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.Comparator;
import java.util.function.IntUnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * create table generated as identity test
 */
class DbCreateTableGeneratedIdentityTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void always() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";
        always(createSql, 1, key -> key + 1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "start", "start with" })
    void always_start(String start) throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (" + start + " 11)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";
        always(createSql, 11, key -> key + 1);
    }

    @ParameterizedTest
    @ValueSource(strings = { "increment", "increment by" })
    void always_increment(String increment) throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (" + increment + " 2)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";
        always(createSql, 1, key -> key + 2);
    }

    @ParameterizedTest
    @ValueSource(strings = { "increment", "increment by" })
    void always_increment_reverse(String increment) throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (maxvalue 100 start 10 " + increment + " -1)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";
        always(createSql, 10, key -> key - 1);
    }

    @Test
    void always_minvalue() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (minvalue 0)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";
        always(createSql, 0, key -> key + 1);
    }

    private void always(String createSql, int keyStart, IntUnaryOperator keyIncrement) throws Exception {
        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " (bar, zzz) values(22, 'b')"));
//TODO  assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(default, 33, 'c')"));
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndGetCount("insert into " + TEST + " values(4, 44, 'd')");
        });
        assertEqualsCode(SqlServiceCode.RESTRICTED_OPERATION_EXCEPTION, e);

        var list = selectAllFromTest();
        assertEquals(3, list.size());

        if (keyIncrement.applyAsInt(keyStart) < keyStart) {
            list.sort(Comparator.comparing(TestEntity::getFoo, Comparator.reverseOrder()));
        }

        int i = 0;
        int key = keyStart;
        {
            var entity = list.get(i++);
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key = keyIncrement.applyAsInt(key);
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key = keyIncrement.applyAsInt(key);
            assertEquals(key, entity.getFoo());
            assertEquals(22, entity.getBar());
            assertEquals("b", entity.getZzz());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "cycle" })
    void always_maxvalue(String cycle) throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (maxvalue 3 " + cycle + ")," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndGetCount("insert into " + TEST + " default values");
        });
        assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);

        var list = selectAllFromTest();
        assertEquals(3, list.size());
        int key = 1;
        for (var entity : list) {
            assertEquals(key, entity.getFoo());
            key++;
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "cycle" })
    void always_cycle(String cycle) throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (maxvalue 3 " + cycle + ")," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));

        assertEquals(1, tm.executeAndGetCount("delete from " + TEST + " where foo = 1"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " (zzz) values('second')"));
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            tm.executeAndGetCount("insert into " + TEST + " (zzz) values('second')");
        });
        assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);

        var list = selectAllFromTest();
        assertEquals(3, list.size());
        int key = 1;
        for (var entity : list) {
            assertEquals(key, entity.getFoo());
            if (key == 1) {
                assertEquals("second", entity.getZzz());
            } else {
                assertEquals("a", entity.getZzz());
            }
            key++;
        }
    }

    @Test
    void always_cycle_upsert() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (maxvalue 3 cycle)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " (zzz) values('second')"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " (zzz) values('second')"));

        var list = selectAllFromTest();
        assertEquals(3, list.size());
        int key = 1;
        for (var entity : list) {
            assertEquals(key, entity.getFoo());
            if (key <= 2) {
                assertEquals("second", entity.getZzz());
            } else {
                assertEquals("a", entity.getZzz());
            }
            key++;
        }
    }

    @Test
    void always_noCycle() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated always as identity (maxvalue 3 no cycle)," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));

        assertEquals(1, tm.executeAndGetCount("delete from " + TEST + " where foo = 1"));
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " (zzz) values('second')"));
        });
        assertEqualsCode(SqlServiceCode.VALUE_EVALUATION_EXCEPTION, e);

        var list = selectAllFromTest();
        assertEquals(2, list.size());
        int key = 2;
        for (var entity : list) {
            assertEquals(key, entity.getFoo());
            key++;
        }
    }

    @Test
    void byDefault() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated by default as identity," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " (bar, zzz) values(22, 'b')"));
        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(4, 44, 'd')"));

        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        });
        assertEqualsCode(SqlServiceCode.UNIQUE_CONSTRAINT_VIOLATION_EXCEPTION, e);

        assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " default values"));
        // TODO assertEquals(1, tm.executeAndGetCount("insert into " + TEST + " values(default, 99, 'q')"));

        var list = selectAllFromTest();
        assertEquals(5, list.size());

        int i = 0;
        int key = 1;
        {
            var entity = list.get(i++);
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(22, entity.getBar());
            assertEquals("b", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key = 4;
            assertEquals(key, entity.getFoo());
            assertEquals(44, entity.getBar());
            assertEquals("d", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
    }

    @Test
    void byDefault_upsert() throws Exception {
        var createSql = "create table " + TEST + "(" //
                + " foo int primary key generated by default as identity," //
                + " bar bigint default 100," //
                + " zzz varchar(10) default 'a'" //
                + ")";

        var tm = createTransactionManagerOcc(getSession());
        tm.executeDdl(createSql);

        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " (bar, zzz) values(22, 'b')"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " values(4, 44, 'd')"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " (zzz) values('overwrite')"));
        assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " default values"));
        // TODO assertEquals(1, tm.executeAndGetCount("insert or replace into " + TEST + " values(default, 99, 'q')"));

        var list = selectAllFromTest();
        assertEquals(5, list.size());

        int i = 0;
        int key = 1;
        {
            var entity = list.get(i++);
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(22, entity.getBar());
            assertEquals("b", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key = 4;
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("overwrite", entity.getZzz());
        }
        {
            var entity = list.get(i++);
            key++;
            assertEquals(key, entity.getFoo());
            assertEquals(100, entity.getBar());
            assertEquals("a", entity.getZzz());
        }
    }
}
