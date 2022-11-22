package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.tsurugidb.iceaxe.result.TsurugiResultEntity;
import com.tsurugidb.iceaxe.result.TsurugiResultSet;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.tsubakuro.channel.common.connection.wire.impl.ResponseBox;

/**
 * slot limit test
 */
@Disabled // TODO remove Disabled ResultSet.close()タイムアウト実装待ち
public class DbSlotLimitTest extends DbTimetoutTest {

    private static final int ATTEMPT_SIZE = ResponseBox.responseBoxSize() + 100;

    @BeforeEach
    void beforeEach(TestInfo info) throws IOException {
        LOG.debug("{} init start", info.getDisplayName());

        dropTestTable();
        createTestTable();

        LOG.debug("{} init end", info.getDisplayName());
    }

    @Test
    void slotLimit() throws IOException {
        testTimeout(new TimeoutModifier());
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        try (var transaction = session.createTransaction(TgTxOption.ofOCC())) {
            transaction.getLowTransaction();

            try (var ps = session.createPreparedQuery(SELECT_SQL)) {
                var rsList = new ArrayList<TsurugiResultSet<?>>();

                pipeServer.setPipeWrite(false);
                try {
                    execute(transaction, ps, rsList);
                } finally {
                    pipeServer.setPipeWrite(true);
                }

                int i = 0;
                for (var rs : rsList) {
                    LOG.trace("close i={}", i);
                    rs.close(); // FIXME Tsubakuroのタイムアウトが未実装なので、ここで待ちに入る
                    i++;
                }
            }
        }
    }

    private void execute(TsurugiTransaction transaction, TsurugiPreparedStatementQuery0<TsurugiResultEntity> ps, List<TsurugiResultSet<?>> rsList) throws IOException, TsurugiTransactionException {
        for (int i = 0; i < ATTEMPT_SIZE; i++) {
            LOG.trace("i={}", i);
            try {
                var rs = ps.execute(transaction);
                rs.setRsConnectTimeout(1, TimeUnit.MILLISECONDS);
                rs.setRsCloseTimeout(1, TimeUnit.MILLISECONDS);
                rsList.add(rs);
            } catch (Throwable t) {
                LOG.error("excption occurred. i={}", i, t);
                throw t;
            }
        }
//      fail("slot limit over did not occur");
    }
}
