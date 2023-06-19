package com.tsurugidb.iceaxe.test.table;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * table list test
 */
@Disabled // TODO remove Disabled. listTables()
class DbTableListTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbTableListTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();

        logInitEnd(LOG, info);
    }

    @Test
    void getTableNameList() throws Exception {
        var session = getSession();
        List<String> tableList = session.getTableNameList();
        System.out.println(tableList);
        assertTrue(tableList.contains(TEST));
    }
}
