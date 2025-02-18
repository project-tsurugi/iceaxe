/*
 * Copyright 2023-2025 Project Tsurugi.
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

import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.option.TgTmTxOption;
import com.tsurugidb.iceaxe.transaction.status.TgTxStatus;
import com.tsurugidb.iceaxe.util.IceaxeInternal;

/**
 * Tsurugi TransactionManager Retry Over Exception.
 */
@SuppressWarnings("serial")
public class TsurugiTmRetryOverIOException extends TsurugiTmIOException {

    /**
     * Creates a new instance.
     *
     * @param transaction  transaction
     * @param cause        the cause
     * @param status       transaction status
     * @param nextTmOption next transaction option
     */
    @IceaxeInternal
    public TsurugiTmRetryOverIOException(TsurugiTransaction transaction, Exception cause, TgTxStatus status, TgTmTxOption nextTmOption) {
        super("transaction retry over", transaction, cause, status, nextTmOption);
    }
}
