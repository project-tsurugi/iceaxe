package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.metadata.TsurugiTableListHelper;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * table metadata close timeout test
 */
public class DbTimeoutTableListCloseTest extends DbTimetoutTest {

    @Test
    void timeoutDefault() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws Exception {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionOption sessionOption) {
                sessionOption.setTimeout(TgTimeoutKey.TABLE_LIST_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var helper = new TsurugiTableListHelper() {
            @Override
            protected FutureResponse<TableList> getLowTableList(SqlClient lowSqlClient) throws IOException {
                var future = super.getLowTableList(lowSqlClient);
                return new FutureResponse<TableList>() {
                    @Override
                    public boolean isDone() {
                        return future.isDone();
                    }

                    @Override
                    public TableList get() throws IOException, ServerException, InterruptedException {
                        throw new UnsupportedOperationException("do not use");
                    }

                    @Override
                    public TableList get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
                        return future.get(timeout, unit);
                    }

                    @Override
                    public void close() throws IOException, ServerException, InterruptedException {
                        pipeServer.setPipeWrite(false);
                        try {
                            future.close();
                        } finally {
                            pipeServer.setPipeWrite(true);
                        }
                    }
                };
            }
        };
        session.setTableListHelper(helper);

        try {
            session.getTableNameList();
        } catch (IOException e) {
            // TABLE_LIST_CLOSEはタイムアウトするような通信処理が無い
//          assertInstanceOf(TimeoutException.class, e.getCause());
//          LOG.trace("timeout success");
//          return;
            throw e;
        }
//      fail("didn't time out");
    }
}
