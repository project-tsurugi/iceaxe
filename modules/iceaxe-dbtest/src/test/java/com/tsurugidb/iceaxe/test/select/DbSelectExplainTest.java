package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * explain select test
 */
class DbSelectExplainTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        var LOG = LoggerFactory.getLogger(DbSelectExplainTest.class);
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @Test
    void pareparedStatement() throws Exception {
        var sql = "select * from " + TEST;

        var session = getSession();
        try (var ps = session.createQuery(sql)) {
            assertThrowsExactly(UnsupportedOperationException.class, () -> {
                var result = ps.explain();
                assertExplain(result);
            }); // TODO explain実装待ち
        }
    }

    @Test
    void psParameter() throws Exception {
        var foo = TgBindVariable.ofInt("foo");
        var sql = "select * from " + TEST + " where foo=" + foo;
        var parameterMapping = TgParameterMapping.of(foo);

        var session = getSession();
        try (var ps = session.createQuery(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(foo.bind(1));
            var result = ps.explain(parameter);
            assertExplain(result);
        }
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertNotNull(actual.getLowPlanGraph());

        var list = actual.getLowColumnList();
        assertEquals(3, list.size());
        var c0 = list.get(0);
        assertEquals("foo", c0.getName());
        assertEquals(AtomType.INT4, c0.getAtomType());
        var c1 = list.get(1);
        assertEquals("bar", c1.getName());
        assertEquals(AtomType.INT8, c1.getAtomType());
        var c2 = list.get(2);
        assertEquals("zzz", c2.getName());
        assertEquals(AtomType.CHARACTER, c2.getAtomType());
    }

    @Test
    void largeResult() throws IOException, PlanGraphException {
        var sql = "select * from " + Stream.generate(() -> TEST).limit(6).collect(Collectors.joining(","));

        var session = getSession();
        try (var ps = session.createQuery(sql, TgParameterMapping.of())) {
            var result = ps.explain(TgBindParameters.of());
            assertNotNull(result.getLowPlanGraph());
        }
    }
}
