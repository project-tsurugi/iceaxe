package com.tsurugidb.iceaxe.test.delete;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * explain delete test
 */
class DbDeleteExplainTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws IOException {
        var LOG = LoggerFactory.getLogger(DbDeleteExplainTest.class);
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void pareparedStatement() throws Exception {
        var sql = "delete from " + TEST;

        var session = getSession();
        try (var ps = session.createStatement(sql)) {
            assertThrowsExactly(UnsupportedOperationException.class, () -> {
                var result = ps.explain();
                assertExplain(result);
            }); // TODO explain実装待ち
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

        var list = actual.getLowColumnList();
        assertEquals(0, list.size());
    }
}
