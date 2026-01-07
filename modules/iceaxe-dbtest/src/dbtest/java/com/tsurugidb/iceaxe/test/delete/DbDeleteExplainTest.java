package com.tsurugidb.iceaxe.test.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * explain delete test
 */
class DbDeleteExplainTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbDeleteExplainTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void pareparedStatement() throws Exception {
        var sql = "delete from " + TEST;

        var session = getSession();
        try (var ps = session.createStatement(sql)) {
            var result = ps.explain();
            assertExplain(result);
        }
    }

    @Test
    void psParameter() throws Exception {
        var bar = TgBindVariable.ofLong("bar");
        var sql = "delete from " + TEST + " where bar=" + bar;
        var parameterMapping = TgParameterMapping.of(bar);

        var session = getSession();
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(bar.bind(1));
            var result = ps.explain(parameter);
            assertExplain(result);
        }
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertNotNull(actual.getLowPlanGraph());

        var list = actual.getColumnList();
        assertEquals(0, list.size());
    }
}
