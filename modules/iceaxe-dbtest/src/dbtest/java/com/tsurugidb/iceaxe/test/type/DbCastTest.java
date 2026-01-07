package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.exception.ServerException;

/**
 * cast test
 */
class DbCastTest extends DbTestTableTester {

    @BeforeAll
    static void beforeAll(TestInfo info) throws Exception {
        var LOG = LoggerFactory.getLogger(DbCastTest.class);
        logInitStart(LOG, info);

        dropTestTable();
        createTestTable();
        insertTestTable(1);

        logInitEnd(LOG, info);
    }

    @Test
    void cast_from_bigint() throws Exception {
        test("123", "int", 123);
        test("123", "bigint", 123L);
        test("123", "double", 123d);
        test("123", "decimal", BigDecimal.valueOf(123));
        test("123", "decimal(5,2)", new BigDecimal("123.00"));
        test("123", "decimal(2)", BigDecimal.valueOf(99));
        test("123", "char(5)", "123  ");
        test("123", "char", "1");
        test("123", "varchar(10)", "123");
        test("123", "varchar", "123");
    }

    @Test
    void cast_from_double() throws Exception {
        test("123e0", "int", 123);
        test("123e0", "bigint", 123L);
        test("123e0", "double", 123d);
        test("123.4e0", "decimal", BigDecimal.valueOf(123));
        test("123e0", "decimal(5,2)", new BigDecimal("123.00"));
        test("123.4e0", "decimal(5,2)", new BigDecimal("123.40"));
        test("123.4e0", "decimal(4,3)", BigDecimal.valueOf(9.999));
        test("123e0", "char(5)", "123  ");
        test("123e0", "char", "1");
        test("123e0", "varchar(10)", "123");
        test("123e0", "varchar", "123");
    }

    @Test
    void cast_from_decimal() throws Exception {
        test("123.0", "int", 123);
        test("123.0", "bigint", 123L);
        test("123.0", "double", 123d);
        test("123.4", "decimal", BigDecimal.valueOf(123));
        test("123.0", "decimal(5,2)", new BigDecimal("123.00"));
        test("123.4", "decimal(5,2)", new BigDecimal("123.40"));
        test("123.4", "decimal(4,3)", BigDecimal.valueOf(9.999));
        test("123.0", "char(7)", "123.0  ");
        test("123.00", "char(7)", "123.00 ");
        test("123.0", "char", "1");
        test("123.0", "varchar(10)", "123.0");
        test("123.0", "varchar", "123.0");
    }

    @Test
    void cast_from_varchar() throws Exception {
        test("'123'", "int", 123);
        test("'123'", "bigint", 123L);
        test("'123'", "double", 123d);
        test("'123.4'", "decimal", BigDecimal.valueOf(123));
        test("'123'", "decimal(5,2)", new BigDecimal("123.00"));
        test("'123.4'", "decimal(5,2)", new BigDecimal("123.40"));
        test("'123.4'", "decimal(4,3)", BigDecimal.valueOf(9.999));
        test("'123'", "char(5)", "123  ");
        test("'123'", "char", "1");
        test("'123'", "varchar(10)", "123");
        test("'123'", "varchar", "123");
    }

    private <T> void test(String value, String type, T expected) throws IOException, InterruptedException {
        var session = getSession();
        var tm = createTransactionManagerOcc(session);

        for (var s : List.of("cast", "::")) {
            var sql = "select " + cast(s, value, type) + " from " + TEST;
            @SuppressWarnings("unchecked")
            var clazz = (Class<T>) expected.getClass();

            tm.execute(transaction -> {
                try (var ps = session.createQuery(sql, TgResultMapping.ofSingle(clazz)); //
                        var rs = transaction.executeQuery(ps)) {
                    var lowMetadata = rs.getLowResultSet().getMetadata();
                    var lowtype = lowMetadata.getColumns().get(0).getAtomType();
                    assertEquals(TgDataType.of(clazz).getLowDataType(), lowtype);

                    T actual = rs.findRecord().get();
                    assertEquals(expected, actual);
                } catch (ServerException e) {
                    throw new TsurugiTransactionException(e);
                }
            });
        }
    }

    private String cast(String s, String value, String type) {
        switch (s) {
        case "cast":
            return "cast(" + value + " as " + type + ")";
        case "::":
            return value + "::" + type;
        default:
            throw new AssertionError(s);
        }
    }
}
