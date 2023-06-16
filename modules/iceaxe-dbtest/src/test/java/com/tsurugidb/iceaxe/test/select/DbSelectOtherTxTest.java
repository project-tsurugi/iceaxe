package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableLong;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select LTX (update other transaction) test
 */
class DbSelectOtherTxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        logInitEnd(info);
    }

    @RepeatedTest(8)
    void select1() throws Exception {
        var selectSql = "select * from " + TEST + " where foo=1";
        var bar = TgBindVariable.ofLong("bar");
        var updateSql = "update " + TEST + " set bar=" + bar + " where foo=1";

        var session = getSession();
        try (var selectPs = session.createQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createStatement(updateSql, TgParameterMapping.of(bar))) {
            updateOtherTxOcc(session, updatePs, bar, 111);

            try (var tx = session.createTransaction(TgTxOption.ofLTX(TEST))) {
//              var entity1 = select(tx, selectPs); // do not call
//              assertEquals(111L, entity1.getBar());

                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    updateOtherTxOcc(session, updatePs, bar, 222);
                });
//              assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e); // TODO ERR_CONFLICT_ON_WRITE_PRESERVEは無くなるかも
                var code = findDiagnosticCode(e);
                var expectedCode = List.of(SqlServiceCode.ERR_SERIALIZATION_FAILURE, SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE);
                if (!expectedCode.contains(code)) {
                    fail(MessageFormat.format("expected: {1} but was: <{0}>", code, expectedCode));
                }

                var entity2 = select(tx, selectPs);
                assertEquals(111L, entity2.getBar());

                tx.commit(TgCommitType.DEFAULT);
            }

            try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                var entity3 = select(tx, selectPs);
                assertEquals(111L, entity3.getBar());

                tx.commit(TgCommitType.DEFAULT);
            }
        }
    }

    @RepeatedTest(4)
    void select2() throws Exception {
        var selectSql = "select * from " + TEST + " where foo=1";
        var bar = TgBindVariable.ofLong("bar");
        var updateSql = "update " + TEST + " set bar=" + bar + " where foo=1";

        var session = getSession();
        try (var selectPs = session.createQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createStatement(updateSql, TgParameterMapping.of(bar))) {
            updateOtherTxOcc(session, updatePs, bar, 111);

            try (var tx = session.createTransaction(TgTxOption.ofLTX(TEST))) {
                var entity1 = select(tx, selectPs);
                assertEquals(111L, entity1.getBar());

                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    updateOtherTxOcc(session, updatePs, bar, 222);
                });
                assertEqualsCode(SqlServiceCode.ERR_SERIALIZATION_FAILURE, e);

                var entity2 = select(tx, selectPs);
                assertEquals(111L, entity2.getBar());

                tx.commit(TgCommitType.DEFAULT);
            }

            try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
                var entity3 = select(tx, selectPs);
                assertEquals(111L, entity3.getBar());

                tx.commit(TgCommitType.DEFAULT);
            }
        }
    }

    private void updateOtherTxOcc(TsurugiSession session, TsurugiSqlPreparedStatement<TgBindParameters> updatePs, TgBindVariableLong bar, long value)
            throws IOException, TsurugiTransactionException, InterruptedException {
        try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgBindParameters.of(bar.bind(value));
            int count = tx.executeAndGetCount(updatePs, parameter);
            assertUpdateCount(1, count);
            tx.commit(TgCommitType.DEFAULT);
        }
    }

    private TestEntity select(TsurugiTransaction tx, TsurugiSqlQuery<TestEntity> selectPs) throws IOException, TsurugiTransactionException, InterruptedException {
        return tx.executeAndFindRecord(selectPs).get();
    }
}
