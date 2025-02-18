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
package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * Always the same {@link TgTxOption}.
 */
@ThreadSafe
public class TgTmTxOptionAlways extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionAlways.
     *
     * @param txOption        transaction option
     * @param attemptMaxCount attempt max count
     * @return TgTmTxOptionAlways
     */
    public static TgTmTxOptionAlways of(TgTxOption txOption, int attemptMaxCount) {
        if (txOption == null) {
            throw new IllegalArgumentException("txOption is null");
        }
        return new TgTmTxOptionAlways(txOption, attemptMaxCount);
    }

    /** transaction option */
    protected final TgTxOption txOption;
    /** attempt max count */
    protected final int attemptMaxCount;

    /**
     * Creates a new instance.
     *
     * @param txOption        transaction option
     * @param attemptMaxCount attempt max count
     */
    public TgTmTxOptionAlways(TgTxOption txOption, int attemptMaxCount) {
        this.txOption = txOption;
        this.attemptMaxCount = attemptMaxCount;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        return TgTmTxOption.execute(txOption, null);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException exception, TgTmRetryInstruction retryInstruction) {
        if (attempt < attemptMaxCount) {
            return TgTmTxOption.execute(txOption, retryInstruction);
        }
        return TgTmTxOption.retryOver(retryInstruction);
    }

    @Override
    protected String getDefaultDescription() {
        return "always(" + txOption + ", " + attemptMaxCount + ")";
    }
}
