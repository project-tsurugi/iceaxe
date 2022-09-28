package com.tsurugidb.iceaxe.test.timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

import com.tsurugidb.iceaxe.metadata.TsurugiTableMetadataHelper;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * table metadata close timeout test
 */
public class DbTimeoutTableMetadataCloseTest extends DbTimetoutTest {

    @Test
    void timeoutDefault() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.DEFAULT, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Test
    void timeoutSpecified() throws IOException {
        testTimeout(new TimeoutModifier() {
            @Override
            public void modifySessionInfo(TgSessionInfo info) {
                info.timeout(TgTimeoutKey.METADATA_CLOSE, 1, TimeUnit.SECONDS);
            }
        });
    }

    @Override
    protected void clientTask(PipeServerThtread pipeServer, TsurugiSession session, TimeoutModifier modifier) throws Exception {
        var helper = new TsurugiTableMetadataHelper() {
            @Override
            protected FutureResponse<TableMetadata> getLowTableMetadata(SqlClient lowSqlClient, String tableName) throws IOException {
                var future = super.getLowTableMetadata(lowSqlClient, tableName);
                return new FutureResponse<TableMetadata>() {
                    @Override
                    public boolean isDone() {
                        return future.isDone();
                    }

                    @Override
                    public TableMetadata get() throws IOException, ServerException, InterruptedException {
                        throw new UnsupportedOperationException("do not use");
                    }

                    @Override
                    public TableMetadata get(long timeout, TimeUnit unit) throws IOException, ServerException, InterruptedException, TimeoutException {
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
        DbTimeoutTableMetadataConnectTest.setHelper(session, helper);

        try {
            session.findTableMetadata(TEST);
        } catch (IOException e) {
            // METADATA_CLOSEはタイムアウトするような通信処理が無い
//          assertInstanceOf(TimeoutException.class, e.getCause());
//          LOG.trace("timeout success");
//          return;
            throw e;
        }
//      fail("didn't time out");
    }
}
