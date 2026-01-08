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
package com.tsurugidb.iceaxe.transaction.manager.exception;

import java.io.IOException;

import com.tsurugidb.iceaxe.exception.TsurugiIOException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.iceaxe.transaction.status.TgTxStatus;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi TransactionManager IOException.
 */
@SuppressWarnings("serial")
public class TsurugiTmIOException extends TsurugiIOException {

    private final TsurugiTransaction transaction;
    private final TgTxStatus status;
    private final TgTmTxOption nextTmOption;

    /**
     * Creates a new instance.
     *
     * @param message      the detail message
     * @param transaction  transaction
     * @param cause        the cause
     * @param status       transaction status
     * @param nextTmOption next transaction option
     */
    @IceaxeInternal
    public TsurugiTmIOException(String message, TsurugiTransaction transaction, Exception cause, TgTxStatus status, TgTmTxOption nextTmOption) {
        super(createMessage(message, transaction, nextTmOption), cause);
        this.transaction = transaction;
        this.status = status;
        this.nextTmOption = nextTmOption;
    }

    private static String createMessage(String message, TsurugiTransaction transaction, TgTmTxOption nextTmOption) {
        return message + ". " + transaction + ", nextTx=" + nextTmOption;
    }

    @Override
    public int getIceaxeTxId() {
        return transaction.getIceaxeTxId();
    }

    @Override
    public int getIceaxeTmExecuteId() {
        return transaction.getIceaxeTmExecuteId();
    }

    @Override
    public int getAttempt() {
        return transaction.getAttempt();
    }

    @Override
    public TgTxOption getTransactionOption() {
        return transaction.getTransactionOption();
    }

    @Override
    public String getTransactionId() {
        try {
            return transaction.getTransactionId();
        } catch (IOException | InterruptedException e) {
            return null;
        }
    }

    /**
     * get transaction status.
     *
     * @return transaction status
     */
    public TgTxStatus getTransactionStatus() {
        return this.status;
    }

    /**
     * get next transaction option.
     *
     * @return transaction option
     */
    public TgTmTxOption getNextTmOption() {
        return this.nextTmOption;
    }
}
