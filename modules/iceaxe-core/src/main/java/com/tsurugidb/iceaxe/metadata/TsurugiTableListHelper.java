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
 * Tsurugi table list helper.
 */
public class TsurugiTableListHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiTableListHelper.class);

    /**
     * get table list.
     *
     * @param session tsurugi session
     * @return table list
     * @throws IOException          if an I/O error occurs while retrieving table list
     * @throws InterruptedException if interrupted while retrieving table list
     */
    public TgTableList getTableList(TsurugiSession session) throws IOException, InterruptedException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableList start");
        var lowTableListFuture = getLowTableList(lowSqlClient);
        LOG.trace("getTableList started");
        return getTableList(session, lowTableListFuture);
    }

    /**
     * get low table list.
     *
     * @param lowSqlClient low SQL client
     * @return future of table list
     * @throws IOException if an I/O error occurs while retrieving table list
     */
    protected FutureResponse<TableList> getLowTableList(SqlClient lowSqlClient) throws IOException {
        return lowSqlClient.listTables();
    }

    /**
     * get table list.
     *
     * @param session            tsurugi session
     * @param lowTableListFuture future of low table list
     * @return table list
     * @throws IOException          if an I/O error occurs while retrieving table list
     * @throws InterruptedException if interrupted while retrieving table list
     */
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

    /**
     * get connect timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getConnectTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_LIST_CONNECT);
    }

    /**
     * get close timeout.
     *
     * @param sessionOption session option
     * @return timeout
     */
    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_LIST_CLOSE);
    }

    /**
     * Creates a new table list instance.
     *
     * @param lowTableList low table list
     * @return table list
     */
    protected TgTableList newTableList(TableList lowTableList) {
        return new TgTableList(lowTableList);
    }
}
