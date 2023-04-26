package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.result.TsurugiQueryResult;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;
import com.tsurugidb.tsubakuro.exception.ResponseTimeoutException;

/**
 * slot limit test
 */
public class DbSlotLimitTest extends DbTimetoutTest {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

    @BeforeEach
    void beforeEach(TestInfo info) throws Exception {
        logInitStart(info);

        dropTestTable();
        createTestTable();

        logInitEnd(info);
    }

    @Test
    void slotLimit() throws Exception {
        testTimeout(new TimeoutModifier());
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var transaction = session.createTransaction(TgTxOption.ofOCC());
        try {
            transaction.setCloseTimeout(1, TimeUnit.SECONDS); // TODO 本来はトランザクションはタイムアウトせず正常にクローズできて欲しい
            transaction.getLowTransaction();

            try (var ps = session.createQuery(SELECT_SQL)) {
                var resultList = new ArrayList<TsurugiQueryResult<?>>();

                pipeServer.setPipeWrite(false);
                try {
                    execute(transaction, ps, resultList);
                } finally {
                    pipeServer.setPipeWrite(true);
                }

                int i = 0;
                for (var result : resultList) {
                    LOG.trace("close i={}", i);
                    try {
                        result.close();
                    } catch (ResponseTimeoutException e) {
                        // success
                    }
                    i++;
                }
            }
        } finally {
            try {
                transaction.close();
            } catch (ResponseTimeoutException e) {
                // TODO 本来はタイムアウトせず正常にクローズできて欲しい
                LOG.warn("transaction.close() {}", e.getClass().getName());
            }
        }
    }

    private void execute(TsurugiTransaction transaction, TsurugiSqlQuery<TsurugiResultEntity> ps, List<TsurugiQueryResult<?>> resultList)
            throws IOException, InterruptedException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            LOG.trace("i={}", i);
            try {
                var result = ps.execute(transaction);
                result.setRsConnectTimeout(1, TimeUnit.MILLISECONDS);
                result.setRsCloseTimeout(1, TimeUnit.MILLISECONDS);
                resultList.add(result);
            } catch (Throwable t) {
                LOG.error("excption occurred. i={}", i, t);
                throw t;
            }
        }
//      fail("slot limit over did not occur");
    }
}
