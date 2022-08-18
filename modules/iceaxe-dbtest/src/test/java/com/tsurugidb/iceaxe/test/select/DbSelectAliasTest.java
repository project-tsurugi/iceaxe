package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * select alias test
 */
class DbSelectAliasTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeAll
    static void beforeAll() throws IOException {
        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);
    }

    @Test
    void selectAsName() throws IOException {
        var sql = "select count(*) as count from " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("count"), nameList);
            return record.getInt4("count");
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int cnt = ps.executeAndFindRecord(tm).get();
            assertEquals(SIZE, cnt);
        }
    }

    @Test
    void selectName() throws IOException {
        var sql = "select count(*) as cnt from " + TEST; // TODO asを削除
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("cnt"), nameList);
            return record.getInt4("cnt");
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int cnt = ps.executeAndFindRecord(tm).get();
            assertEquals(SIZE, cnt);
        }
    }

    @Test
    void selectNoName() throws IOException {
        var sql = "select count(*) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("$0"), nameList);
            return record.getInt4("$0");
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            int cnt = ps.executeAndFindRecord(tm).get();
            assertEquals(SIZE, cnt);
        }
    }

    @Test
    void selectNoName2() throws IOException {
        var sql = "select count(*), count(foo) from " + TEST;
        var resultMapping = TgResultMapping.of(record -> {
            List<String> nameList = record.getNameList();
            assertEquals(List.of("$0", "$1"), nameList);
            return TsurugiResultEntity.of(record);
        });

        var session = getSession();
        var tm = createTransactionManagerOcc(session);
        try (var ps = session.createPreparedQuery(sql, resultMapping)) {
            tm.execute(transaction -> {
                try (var rs = ps.execute(transaction)) {
                    List<String> nameList = rs.getNameList();
                    assertEquals(List.of("$0", "$1"), nameList);
                    TsurugiResultEntity entity = rs.findRecord().get();
                    assertEquals(SIZE, entity.getInt4("$0"));
                    assertEquals(SIZE, entity.getInt4("$1"));
                }
            });
        }
    }
}
