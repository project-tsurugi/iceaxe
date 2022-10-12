package com.tsurugidb.iceaxe.explain;

import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.iceaxe.session.TgSessionInfo.TgTimeoutKey;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.util.IceaxeIoUtil;
import com.tsurugidb.iceaxe.util.IceaxeTimeout;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.StatementMetadata;
import com.tsurugidb.tsubakuro.util.FutureResponse;

/**
 * Tsurugi statement metadata helper
 */
public class TsurugiExplainHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TsurugiExplainHelper.class);

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session tsurugi session
     * @param source  SQL statement
     * @return statement metadata
     * @throws IOException
     */
    public TgStatementMetadata explain(TsurugiSession session, String source) throws IOException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, source);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, lowStatementMetadataFuture);
    }

    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, String source) throws IOException {
        return lowSqlClient.explain(source);
    }

    /**
     * Retrieves execution plan of the statement.
     *
     * @param session      tsurugi session
     * @param source       SQL statement
     * @param lowPs        PreparedStatement
     * @param lowParameter list of Parameter
     * @return statement metadata
     * @throws IOException
     */
    public TgStatementMetadata explain(TsurugiSession session, String source, PreparedStatement lowPs, List<Parameter> lowParameter) throws IOException {
        var lowSqlClient = session.getLowSqlClient();
        LOG.trace("explain start. source={}", source);
        var lowStatementMetadataFuture = explainLow(lowSqlClient, lowPs, lowParameter);
        LOG.trace("explain started");
        return getStatementMetadata(session, source, lowStatementMetadataFuture);
    }

    protected FutureResponse<StatementMetadata> explainLow(SqlClient lowSqlClient, PreparedStatement lowPs, List<Parameter> lowParameter) throws IOException {
        return lowSqlClient.explain(lowPs, lowParameter);
    }

    protected TgStatementMetadata getStatementMetadata(TsurugiSession session, String source, FutureResponse<StatementMetadata> lowStatementMetadataFuture) throws IOException {
        try (var closeable = IceaxeIoUtil.closeable(lowStatementMetadataFuture)) {

            var info = session.getSessionInfo();
            var connectTimeout = new IceaxeTimeout(info, TgTimeoutKey.EXPLAIN_CONNECT);
            var closeTimeout = new IceaxeTimeout(info, TgTimeoutKey.EXPLAIN_CLOSE);
            closeTimeout.apply(lowStatementMetadataFuture);

            var lowStatementMetadata = IceaxeIoUtil.getAndCloseFuture(lowStatementMetadataFuture, connectTimeout);
            LOG.trace("explain end");

            return newStatementMetadata(source, lowStatementMetadata);
        }
    }

    protected TgStatementMetadata newStatementMetadata(String source, StatementMetadata lowStatementMetadata) {
        return new TgStatementMetadata(source, lowStatementMetadata);
    }
}
