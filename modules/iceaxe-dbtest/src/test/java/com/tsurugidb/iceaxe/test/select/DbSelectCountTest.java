package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select count test
 */
class DbSelectCountTest extends DbTestTableTester {

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_converter(int size) throws Exception {
        count(size, TgResultMapping.of(record -> record.nextInt()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_class(int size) throws Exception {
        count(size, TgResultMapping.of(int.class));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, 3 })
    void count_singleColumn_type(int size) throws Exception {
        count(size, TgResultMapping.of(TgDataType.INT));
    }

    private void count(int size, TgResultMapping<Integer> resultMapping) throws IOException, InterruptedException {
        insertTestTable(size);

        var sql = "select count(*) from " + TEST;

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createQuery(sql, resultMapping)) {
            int count = tm.executeAndFindRecord(ps).get();
            assertEquals(size, count);
        }
    }
}
