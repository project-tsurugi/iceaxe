package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;

import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TestLowTransaction extends TestServerResource implements Transaction {

    private FutureResponse<Void> commitFuture;
    private FutureResponse<Void> rollbackFuture;
    private FutureResponse<SqlServiceException> transactionStatusFuture;
    private FutureResponse<ResultSet> resultSetFuture;
    private FutureResponse<ExecuteResult> executeResultFuture;

    @Override
    public String getTransactionId() {
        return "TID-TEST" + LocalDateTime.now();
    }

    public void setTestCommitFutureResponse(FutureResponse<Void> future) {
        this.commitFuture = future;
    }

    @Override
    public FutureResponse<Void> commit(CommitStatus status) throws IOException {
        return this.commitFuture;
    }

    public void setTestRollbackFutureResponse(FutureResponse<Void> future) {
        this.rollbackFuture = future;
    }

    @Override
    public FutureResponse<Void> rollback() throws IOException {
        return this.rollbackFuture;
    }

    public void setTestTransactionStatusFutureResponse(FutureResponse<SqlServiceException> future) {
        this.transactionStatusFuture = future;
    }

    @Override
    public FutureResponse<SqlServiceException> getSqlServiceException() throws IOException {
        return this.transactionStatusFuture;
    }

    public void setTestResultSetFutureResponse(FutureResponse<ResultSet> future) {
        this.resultSetFuture = future;
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(String source) throws IOException {
        return this.resultSetFuture;
    }

    @Override
    public FutureResponse<ResultSet> executeQuery(PreparedStatement statement, Collection<? extends Parameter> parameters) throws IOException {
        return this.resultSetFuture;
    }

    public void setTestExecuteResultFutureResponse(FutureResponse<ExecuteResult> future) {
        this.executeResultFuture = future;
    }

    @Override
    public FutureResponse<ExecuteResult> executeStatement(String source) throws IOException {
        return this.executeResultFuture;
    }

    @Override
    public FutureResponse<ExecuteResult> executeStatement(PreparedStatement statement, Collection<? extends Parameter> parameters) throws IOException {
        return this.executeResultFuture;
    }
}
