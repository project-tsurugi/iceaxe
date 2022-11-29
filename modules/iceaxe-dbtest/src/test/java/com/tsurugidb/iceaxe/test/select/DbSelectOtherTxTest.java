package com.tsurugidb.iceaxe.test.select;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.io.IOException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableLong;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.test.util.DbTestTableTester;
import com.tsurugidb.iceaxe.test.util.TestEntity;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

/**
 * select LTX (update other transaction) test
 */
class DbSelectOtherTxTest extends DbTestTableTester {

    private static final int SIZE = 4;

    @BeforeEach
    void beforeEach() throws IOException {
        LOG.debug("init start");

        dropTestTable();
        createTestTable();
        insertTestTable(SIZE);

        LOG.debug("init end");
    }

    @RepeatedTest(8)
    @Disabled // TODO remove Disabled WARN_CONFLICT_ON_WRITE_PRESERVEでtateyama-serverがクラッシュすることがある
    void select1() throws IOException, TsurugiTransactionException {
        var selectSql = "select * from " + TEST + " where foo=1";
        var bar = TgVariable.ofInt8("bar");
        var updateSql = "update " + TEST + " set bar=" + bar + " where foo=1";

        var session = getSession();
        try (var selectPs = session.createPreparedQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createPreparedStatement(updateSql, TgParameterMapping.of(bar))) {
            update(session, updatePs, bar, 111);

            try (var tx = session.createTransaction(TgTxOption.ofLTX(TEST))) {
//              var entity1 = select(tx, selectPs);
//              assertEquals(111L, entity1.getBar());

                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    update(session, updatePs, bar, 222);
                });
                assertEqualsCode(SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE, e);

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

    @Test
    void select2() throws IOException, TsurugiTransactionException {
        var selectSql = "select * from " + TEST + " where foo=1";
        var bar = TgVariable.ofInt8("bar");
        var updateSql = "update " + TEST + " set bar=" + bar + " where foo=1";

        var session = getSession();
        try (var selectPs = session.createPreparedQuery(selectSql, SELECT_MAPPING); //
                var updatePs = session.createPreparedStatement(updateSql, TgParameterMapping.of(bar))) {
            update(session, updatePs, bar, 111);

            try (var tx = session.createTransaction(TgTxOption.ofLTX(TEST))) {
                var entity1 = select(tx, selectPs);
                assertEquals(111L, entity1.getBar());

                var e = assertThrowsExactly(TsurugiTransactionException.class, () -> {
                    update(session, updatePs, bar, 222);
                });
                assertEqualsCode(SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE, e);

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

    private void update(TsurugiSession session, TsurugiPreparedStatementUpdate1<TgParameterList> updatePs, TgVariableLong bar, long value) throws IOException, TsurugiTransactionException {
        try (var tx = session.createTransaction(TgTxOption.ofOCC())) {
            var parameter = TgParameterList.of(bar.bind(value));
            int count = updatePs.executeAndGetCount(tx, parameter);
            assertEquals(-1, count); // TODO 1
            tx.commit(TgCommitType.DEFAULT);
        }
    }

    private TestEntity select(TsurugiTransaction tx, TsurugiPreparedStatementQuery0<TestEntity> selectPs) throws IOException, TsurugiTransactionException {
        return selectPs.executeAndFindRecord(tx).get();
    }
}