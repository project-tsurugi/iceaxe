package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TgSessionOption.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.TableList;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi table list helper
 */
public class TsurugiTableListHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTableListHelper.class);

    /**
     * get table metadata.
     *
     * @param session tsurugi session
     * @return table metadata
     * @throws IOException
     * @throws InterruptedException
     */
    public TgTableList getTableList(TsurugiSession session) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableList start");
        var lowTableListFuture = getLowTableList(lowSqlClient);
        LOG.trace("getTableList started");
        return getTableList(session, lowTableListFuture);
    }

    protected FutureResponse<TableList> getLowTableList(SqlClient lowSqlClient) throws IOException {
        return lowSqlClient.listTables();
    }

    protected TgTableList getTableList(TsurugiSession session, FutureResponse<TableList> lowTableListFuture) throws IOException, InterruptedException {
        try (var closeable = IceaxeIoUtil.closeable(lowTableListFuture)) {

            var sessionOption = session.getSessionOption();
            var connectTimeout = getConnectTimeout(sessionOption);
            var closeTimeout = getCloseTimeout(sessionOption);
            closeTimeout.apply(lowTableListFuture);

            var lowTableList = IceaxeIoUtil.getAndCloseFuture(lowTableListFuture, connectTimeout);
            LOG.trace("getTableList end");

            return newTableList(lowTableList);
        }
    }

    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_LIST_CONNECT);
    }

    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_LIST_CLOSE);
    }

    protected TgTableList newTableList(TableList lowTableList) {
        return new TgTableList(lowTableList);
    }
}
