package com.tsurugidb.iceaxe.metadata;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.exception.IceaxeErrorCode;
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
        var sessionOption = session.getSessionOption();
        var connectTimeout = getConnectTimeout(sessionOption);

        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("getTableList start");
        var lowTableListFuture = getLowTableList(lowSqlClient);
        LOG.trace("getTableList started");
        return getTableList(lowTableListFuture, connectTimeout);
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
    @Deprecated(since = "X.X.X")
    protected IceaxeTimeout getCloseTimeout(TgSessionOption sessionOption) {
        return new IceaxeTimeout(sessionOption, TgTimeoutKey.TABLE_LIST_CLOSE);
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
     * @param lowTableListFuture future of low table list
     * @param connectTimeout     connect timeout
     * @return table list
     * @throws IOException          if an I/O error occurs while retrieving table list
     * @throws InterruptedException if interrupted while retrieving table list
     */
    protected TgTableList getTableList(FutureResponse<TableList> lowTableListFuture, IceaxeTimeout connectTimeout) throws IOException, InterruptedException {
        var lowTableList = IceaxeIoUtil.getAndCloseFuture(lowTableListFuture, //
                connectTimeout, IceaxeErrorCode.TABLE_LIST_CONNECT_TIMEOUT, //
                IceaxeErrorCode.TABLE_LIST_CLOSE_TIMEOUT);
        LOG.trace("getTableList end");

        return newTableList(lowTableList);
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
