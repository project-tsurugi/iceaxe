package com.tsurugidb.iceaxe.test.update;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * explain update test
 */
class DbUpdateExplainTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws IOException {
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
        try (var ps = session.createPreparedStatement(sql)) {
            var result = ps.explain();
            assertExplain(result);
        }
    }

    @Test
    void psParameter() throws Exception {
        var add = TgVariable.ofInt8("add");
        var sql = "update " + TEST + " set bar=bar+" + add;
        var parameterMapping = TgParameterMapping.of(add);

        var session = getSession();
        try (var ps = session.createPreparedStatement(sql, parameterMapping)) {
            var parameter = TgParameterList.of(add.bind(1));
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
