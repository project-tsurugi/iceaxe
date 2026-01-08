/*
 * Copyright 2023-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;

import com.tsurugidb.sql.proto.SqlRequest.CommitOption;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.sql.ExecuteResult;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlServiceException;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.TransactionStatus.TransactionStatusWithMessage;
import com.tsurugidb.tsubakuro.util.FutureResponse;

public class TestLowTransaction extends TestServerResource implements Transaction {

    private FutureResponse<Void> commitFuture;
    private FutureResponse<Void> rollbackFuture = new TestFutureResponse<Void>();
    private FutureResponse<SqlServiceException> sqlServiceExceptionFuture;
    private FutureResponse<TransactionStatusWithMessage> transactionStatusFuture;
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
    public FutureResponse<Void> commit(CommitOption option) throws IOException {
        return this.commitFuture;
    }

    public void setTestRollbackFutureResponse(FutureResponse<Void> future) {
        this.rollbackFuture = future;
    }

    @Override
    public FutureResponse<Void> rollback() throws IOException {
        return this.rollbackFuture;
    }

    public void setTestSqlServiceExceptionFutureResponse(FutureResponse<SqlServiceException> future) {
        this.sqlServiceExceptionFuture = future;
    }

    @Override
    public FutureResponse<SqlServiceException> getSqlServiceException() throws IOException {
        return this.sqlServiceExceptionFuture;
    }

    public void setTestTransactionStatusFutureResponse(FutureResponse<TransactionStatusWithMessage> future) {
        this.transactionStatusFuture = future;
    }

    @Override
    public FutureResponse<TransactionStatusWithMessage> getStatus() throws IOException {
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
