package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * table list test
 */
class DbTableListTest extends DbTestTableTester {

    private static final String TEST2 = "test2";

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTableListTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        dropTable(TEST2);

        logInitEnd(LOG, info);
    }

    @Test
    void getTableNameList() throws Exception {
        var session = getSession();
        {
            List<String> tableList = session.getTableNameList();
            assertFalse(tableList.contains(TEST));
            assertFalse(tableList.contains(TEST2));
        }
        {
            createTestTable();

            List<String> tableList = session.getTableNameList();
            assertTrue(tableList.contains(TEST));
            assertFalse(tableList.contains(TEST2));
        }
        {
            executeDdl(session, CREATE_TEST_SQL.replace(TEST, TEST2));

            List<String> tableList = session.getTableNameList();
            assertTrue(tableList.contains(TEST));
            assertTrue(tableList.contains(TEST2));
        }
    }
}
