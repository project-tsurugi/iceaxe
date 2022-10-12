package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;

/**
 * explain drop table test
 */
class DbDropTableExplainTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    private static final String SQL = "drop table " + TEST;

    @Test
    void drop() throws Exception {
        createTestTable();

        var session = getSession();
        var helper = session.getExplainHelper();
        var result = helper.explain(session, SQL);
        assertExplain(result);
    }

    @Test
    void dropNotExists() throws Exception {
        var session = getSession();
        var helper = session.getExplainHelper();
        var result = helper.explain(session, SQL);
        assertExplain(result);
    }

    private static void assertExplain(TgStatementMetadata actual) throws Exception {
        assertThrows(PlanGraphException.class, () -> {
            actual.getLowPlanGraph();
        });

        var list = actual.getLowColumnList();
        assertEquals(0, list.size());
    }
}
