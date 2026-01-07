package com.tsurugidb.iceaxe.test.insert;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;

/**
 * insert if not exists test
 */
class DbInsertIfNotExistsTest extends DbTestTableTester {

    private static final String INSERT_IF_NOT_EXISTS_SQL = INSERT_SQL.replace("insert", "insert if not exists");

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        if (info.getTestMethod().get().getName().contains("NoPk")) {
            createTestTable(false);
        } else {
            createTestTable();
        }

        logInitEnd(info);
    }

    @Test
    void insert() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_IF_NOT_EXISTS_SQL, INSERT_MAPPING)) {
            for (int i = 0; i < 10; i++) {
                var countDetail = tm.executeAndGetCountDetail(ps, entity);
                assertEquals(1, countDetail.getLowCounterMap().size());
                long count = countDetail.getInsertedCount();
                assertEquals((i == 0) ? 1L : 0L, count);

                assertEqualsTestTable(entity);
            }
        }
    }

    @Test
    void insertNoPk() throws Exception {
        var entity = new TestEntity(123, 456, "abc");

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createStatement(INSERT_IF_NOT_EXISTS_SQL, INSERT_MAPPING)) {
            var expectedList = new ArrayList<TestEntity>();
            for (int i = 0; i < 10; i++) {
                var countDetail = tm.executeAndGetCountDetail(ps, entity);
                assertEquals(1, countDetail.getLowCounterMap().size());
                long count = countDetail.getInsertedCount();
                assertEquals(1L, count);

                expectedList.add(entity);
                assertEqualsTestTable(expectedList);
            }
        }
    }
}
