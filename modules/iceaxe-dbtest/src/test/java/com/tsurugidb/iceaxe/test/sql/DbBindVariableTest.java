package com.tsurugidb.iceaxe.test.sql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * bind variable test
 */
class DbBindVariableTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("{} end start", info.getDisplayName());
    }

    @Test
    void bindInsert() throws IOException {
        bindInsert(":foo", ":bar", ":zzz");
    }

    @Test
    void bindColonlessInsert1() throws IOException {
        bindInsert("f", "b", "z");
    }

    @Test
    void bindColonlessInsert2() throws IOException {
        bindInsert("foo", "bar", "zzz");
    }

    private void bindInsert(String f, String b, String z) throws IOException {
        var sql = "insert into " + TEST //
                + "(" + TEST_COLUMNS + ")" //
                + "values(" + f + ", " + b + ", " + z + ")";
        var parameterMapping = TgParameterMapping.of(TestEntity.class) //
                .addInt(trimColumn(f), TestEntity::getFoo) //
                .addLong(trimColumn(b), TestEntity::getBar) //
                .addString(trimColumn(z), TestEntity::getZzz);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var entity = createTestEntity(SIZE);
            tm.executeAndGetCount(ps, entity);
        }

        assertEqualsTestTable(SIZE + 1);
    }

    @Test
    void bindCSelect() throws IOException {
        bindSelect(":bar");
    }

    @Test
    void bindColonlessSelect1() throws IOException {
        bindSelect("b");
    }

    @Test
    void bindColonlessSelect2() throws IOException {
        bindSelect("bar");
    }

    private void bindSelect(String b) throws IOException {
        var bar = TgBindVariable.ofInt(trimColumn(b));
        var sql = SELECT_SQL + " where foo=" + b;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING)) {
            int key = 2;
            var parameter = TgBindParameters.of(bar.bind(key));
            var list = tm.executeAndGetList(ps, parameter);

            if (b.equals("bar")) {
                assertEquals(SIZE, list.size());
            } else {
                assertEquals(1, list.size());
                assertEquals(createTestEntity(key), list.get(0));
            }
        }
    }

    private static String trimColumn(String s) {
        if (s.startsWith(":")) {
            return s.substring(1);
        }
        return s;
    }

    @Test
    void singleVariable_class() throws IOException {
        singleVariable(TgParameterMapping.of("foo", int.class));
    }

    @Test
    void singleVariable_type() throws IOException {
        singleVariable(TgParameterMapping.of("foo", TgDataType.INT));
    }

    private void singleVariable(TgParameterMapping<Integer> parameterMapping) throws IOException {
        var sql = SELECT_SQL + " where foo = :foo";

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, parameterMapping, SELECT_MAPPING)) {
            int key = 2;
            var list = tm.executeAndGetList(ps, key);

            assertEquals(1, list.size());
            assertEquals(createTestEntity(key), list.get(0));
        }
    }

    @Test
    void emptyBind() throws IOException {
        var parameterMapping = TgParameterMapping.of();
        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(SELECT_SQL, parameterMapping, SELECT_MAPPING)) {
            var list = tm.executeAndGetList(ps, null);

            assertEquals(SIZE, list.size());
        }
    }
}
