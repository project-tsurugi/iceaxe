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
package com.tsurugidb.iceaxe.transaction.manager.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryStandardCode;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * {@link TgTxOption} OCC to LTX.
 */
@ThreadSafe
public class TgTmTxOptionOccLtx extends TgTmTxOptionSupplier {

    /**
     * create TgTmTxOptionOccLtx.
     *
     * @param occOption transaction option for OCC
     * @param occSize   occ size
     * @param ltxOption transaction option for LTX or RTX
     * @param ltxSize   ltx size
     * @return TgTmTxOptionOccLtx
     */
    public static TgTmTxOptionOccLtx of(TgTxOption occOption, int occSize, TgTxOption ltxOption, int ltxSize) {
        checkArguments(occOption, occSize, ltxOption, ltxSize);
        return new TgTmTxOptionOccLtx(occOption, occSize, ltxOption, ltxSize);
    }

    /**
     * check arguments.
     *
     * @param occOption transaction option for OCC
     * @param occSize   occ size
     * @param ltxOption transaction option for LTX or RTX
     * @param ltxSize   ltx size
     * @throws IllegalArgumentException if argument is invalid
     */
    protected static void checkArguments(TgTxOption occOption, int occSize, TgTxOption ltxOption, int ltxSize) throws IllegalArgumentException {
        if (!occOption.isOCC()) {
            throw new IllegalArgumentException("occOption is not OCC");
        }
        if (occSize < 1) {
            throw new IllegalArgumentException("occSize < 1 (size=" + occSize + ")");
        }
        if (!(ltxOption.isLTX() || ltxOption.isRTX())) {
            throw new IllegalArgumentException("ltxOption is not LTX,RTX");
        }
        if (ltxSize < 1) {
            throw new IllegalArgumentException("ltxSize < 1 (size=" + ltxSize + ")");
        }
    }

    private final TgTxOption occOption;
    private final int occSize;
    private final TgTxOption ltxOption;
    private final int ltxSize;

    /**
     * Creates a new instance.
     *
     * @param occOption transaction option for OCC
     * @param occSize   occ size
     * @param ltxOption transaction option for LTX or RTX
     * @param ltxSize   ltx size
     */
    public TgTmTxOptionOccLtx(TgTxOption occOption, int occSize, TgTxOption ltxOption, int ltxSize) {
        checkArguments(occOption, occSize, ltxOption, ltxSize);
        this.occOption = occOption;
        this.occSize = occSize;
        this.ltxOption = ltxOption;
        this.ltxSize = ltxSize;
    }

    static class TgTmTxOptionOccLtxExecuteInfo {
        boolean isOcc = true;
        int occCounter = 0;
    }

    @Override
    public Object createExecuteInfo(int iceaxeTmExecuteId) {
        return new TgTmTxOptionOccLtxExecuteInfo();
    }

    private static TgTmTxOptionOccLtxExecuteInfo info(Object executeInfo) {
        return (TgTmTxOptionOccLtxExecuteInfo) executeInfo;
    }

    @Override
    protected TgTmTxOption computeFirstTmOption(Object executeInfo) {
        info(executeInfo).occCounter++;
        return TgTmTxOption.execute(occOption, null);
    }

    @Override
    protected TgTmTxOption computeRetryTmOption(Object executeInfo, int attempt, TsurugiTransactionException exception, TgTmRetryInstruction retryInstruction) {
        var info = info(executeInfo);

        var reason = retryInstruction.retryCode();
        if (reason == TgTmRetryStandardCode.RETRYABLE_LTX) {
            info.isOcc = false;
        }

        if (info.isOcc) {
            if (attempt < this.occSize) {
                info.occCounter++;
                return TgTmTxOption.execute(occOption, retryInstruction);
            }
            info.isOcc = false;
        }

        int n = attempt - info.occCounter;
        if (n < this.ltxSize) {
            return TgTmTxOption.execute(ltxOption, retryInstruction);
        }

        return TgTmTxOption.retryOver(retryInstruction);
    }

    @Override
    protected String getDefaultDescription() {
        return occOption + "*var(" + occSize + "), " + ltxOption + "*" + ltxSize;
    }
}
