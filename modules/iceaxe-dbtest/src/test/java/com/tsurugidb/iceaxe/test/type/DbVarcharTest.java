package com.tsurugidb.iceaxe.test.type;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableString;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariables;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * varchar test
 */
class DbVarcharTest extends DbTestTableTester {

    private static final int MAX_LENGTH = 30716;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();

        logInitEnd(info);
    }

    @Test
    void createError() throws Exception {
        var session = getSession();

        var createSql = getCreateTable(MAX_LENGTH + 1);
        var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
            executeDdl(session, createSql);
        });
        assertEqualsCode(SqlServiceCode.UNSUPPORTED_RUNTIME_FEATURE_EXCEPTION, e);
        assertContains("character type on column \"value\" is unsupported (invalid length)", e.getMessage());
    }

    private static String getCreateTable(int length) {
        return "create table " + TEST + "(" //
                + "  pk int primary key," //
                + "  value varchar(" + length + ")" //
                + ")";
    }

    @ParameterizedTest
    @ValueSource(ints = { 100, MAX_LENGTH - 1, MAX_LENGTH })
    void insertUpdate(int maxLength) throws Exception {
        new VarcharTester(maxLength).test();
    }

    static class VarcharTester {
        protected final int maxLength;
        private TsurugiSqlPreparedStatement<TgBindParameters> insertPs;
        private TsurugiSqlPreparedQuery<TgBindParameters, TsurugiResultEntity> selectPs;
        private TsurugiSqlPreparedStatement<TgBindParameters> updatePs;
        private TsurugiTransactionManager tm;
        private int size = 0;
        private String prevValue;

        public VarcharTester(int maxLength) {
            this.maxLength = maxLength;
        }

        private static final TgBindVariableInteger PK = TgBindVariable.ofInt("pk");
        private static final TgBindVariableString VALUE = TgBindVariable.ofString("value");

        public void test() throws IOException, InterruptedException {
            var session = getSession();

            var createSql = getCreateSql();
            executeDdl(session, createSql);

            var insertSql = "insert into " + TEST + " values(" + TgBindVariables.toSqlNames(PK, VALUE) + ")";
            var insertMapping = TgParameterMapping.of(PK, VALUE);
            var selectSql = "select * from " + TEST + " where pk=" + PK;
            var selectMapping = TgParameterMapping.of(PK);
            var updateSql = "update " + TEST + " set value=" + VALUE;
            var updateMapping = TgParameterMapping.of(PK, VALUE);
            try (var insertPs = session.createStatement(insertSql, insertMapping); //
                    var selectPs = session.createQuery(selectSql, selectMapping); //
                    var updatePs = session.createStatement(updateSql, updateMapping)) {
                this.insertPs = insertPs;
                this.selectPs = selectPs;
                this.updatePs = updatePs;
                this.tm = createTransactionManagerOcc(session);
                test(0, true);
                test(1, true);
                test(maxLength - 1, true);
                test(maxLength, true);
                test(maxLength + 1, false);
            }
        }

        protected String getCreateSql() {
            return getCreateTable(maxLength);
        }

        private void test(int length, boolean success) throws IOException, InterruptedException {
            String insertValue = createString('a', length);
            String updateValue = createString('b', length);

            { // insert
                var parameter = TgBindParameters.of(PK.bind(length), VALUE.bind(insertValue));
                if (success) {
                    int count = tm.executeAndGetCount(insertPs, parameter);
                    assertUpdateCount(1, count);
                    this.size++;
                } else {
                    var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                        tm.executeAndGetCount(insertPs, parameter);
                    });
                    assertEqualsCode(SqlServiceCode.VALUE_TOO_LONG_EXCEPTION, e);
                    assertContains("Insufficient storage to store field data", e.getMessage()); // TODO エラー詳細情報（カラム名や桁数）
                }
            }
            {
                var parameter = TgBindParameters.of(PK.bind(length));
                var list = tm.executeAndGetList(selectPs, parameter);
                if (success) {
                    assertEquals(1, list.size());
                    var actual = list.get(0);
                    assertEquals(length, actual.getInt("pk"));
                    String expectedValue = getExpectedValue(insertValue);
                    assertEquals(expectedValue, actual.getString("value"));
                } else {
                    assertEquals(0, list.size());
                }
            }
            { // update
                var parameter = TgBindParameters.of(VALUE.bind(updateValue));
                if (success) {
                    int count = tm.executeAndGetCount(updatePs, parameter);
                    assertUpdateCount(this.size, count);
                    this.prevValue = updateValue;
                } else {
                    var e = assertThrowsExactly(TsurugiTmIOException.class, () -> {
                        tm.executeAndGetCount(updatePs, parameter);
                    });
                    assertEqualsCode(SqlServiceCode.VALUE_TOO_LONG_EXCEPTION, e);
                    assertContains("Insufficient storage to store field data", e.getMessage()); // TODO エラー詳細情報（カラム名や桁数）
                }
            }
            {
                var list = tm.executeAndGetList("select * from " + TEST);
                for (var actual : list) {
                    String expectedValue;
                    if (success) {
                        expectedValue = getExpectedValue(updateValue);
                    } else {
                        expectedValue = getExpectedValue(this.prevValue);
                    }
                    assertEquals(expectedValue, actual.getString("value"));
                }
            }
        }

        protected String getExpectedValue(String value) {
            return value;
        }

        protected static String createString(char c, int length) {
            var buf = new byte[length];
            Arrays.fill(buf, (byte) c);
            return new String(buf, StandardCharsets.UTF_8);
        }
    }
}
