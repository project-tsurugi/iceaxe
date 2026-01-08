package com.tsurugidb.iceaxe.test.function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;

/**
 * octet_length function test
 */
class DbOctetLengthTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbOctetLengthTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTable();

        logInitEnd(LOG, info);
    }

    private static void createTable() throws IOException, InterruptedException {
        String sql = "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value varchar(10)" //
                + ")";
        executeDdl(getSession(), sql);
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "a", "abc", "あいう", "null" })
    void test(String s) throws Exception {
        String value = s.equals("null") ? null : s;
        insert(value);

        var tm = createTransactionManagerOcc(getSession());
        var entity = tm.executeAndFindRecord("select octet_length(value) from " + TEST).get();
        Long result = entity.getLongOrNull(0);
        if (value == null) {
            assertNull(result);
        } else {
            assertEquals(s.getBytes(StandardCharsets.UTF_8).length, result.longValue());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "", "a", "abc", "あいう", "null", "\0" })
    void testPlaceholder(String s) throws Exception {
        String value = s.equals("null") ? null : s;
        insert("dummy");

        var tm = createTransactionManagerOcc(getSession());
        var entity = tm.executeAndFindRecord("select octet_length(:v) from " + TEST, //
                TgParameterMapping.ofSingle("v", String.class), //
                value).get();
        Long result = entity.getLongOrNull(0);
        if (value == null) {
            assertNull(result);
        } else {
            assertEquals(s.getBytes(StandardCharsets.UTF_8).length, result.longValue());
        }
    }

    private static void insert(String value) throws IOException, InterruptedException {
        var session = getSession();
        var v = TgBindVariable.ofString("value");
        var insertSql = "insert or replace into " + TEST + " values(1, " + v + ")";
        var insertMapping = TgParameterMapping.of(v);
        try (var ps = session.createStatement(insertSql, insertMapping)) {
            var tm = createTransactionManagerOcc(session);
            tm.execute(transaction -> {
                var parameter = TgBindParameters.of(v.bind(value));
                transaction.executeAndGetCount(ps, parameter);
            });
        }
    }
}
