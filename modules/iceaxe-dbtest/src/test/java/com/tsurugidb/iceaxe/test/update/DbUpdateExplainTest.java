package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

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
 * explain update test
 */
class DbUpdateExplainTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbUpdateExplainTest.class);
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void pareparedStatement() throws Exception {
        var sql = "update " + TEST + " set bar=bar+1";

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
        var add = TgBindVariable.ofLong("add");
        var sql = "update " + TEST + " set bar=bar+" + add;
        var parameterMapping = TgParameterMapping.of(add);

        var session = getSession();
        try (var ps = session.createStatement(sql, parameterMapping)) {
            var parameter = TgBindParameters.of(add.bind(1));
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
