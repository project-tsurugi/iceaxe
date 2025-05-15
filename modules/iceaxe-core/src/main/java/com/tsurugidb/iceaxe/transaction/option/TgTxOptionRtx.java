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
package com.tsurugidb.iceaxe.transaction.option;

import java.util.Objects;
import java.util.OptionalInt;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option (RTX).
 */
@ThreadSafe
public class TgTxOptionRtx extends AbstractTgTxOptionLong<TgTxOptionRtx> {

    private OptionalInt scanParallel = OptionalInt.empty();

    @Override
    public String typeName() {
        return "RTX";
    }

    @Override
    public TransactionType type() {
        return TransactionType.READ_ONLY;
    }

    /**
     * Set scan parallel.
     *
     * @param scanParallel scan parallel
     * @return this
     * @since 1.9.0
     */
    public synchronized TgTxOptionRtx scanParallel(int scanParallel) {
        this.scanParallel = OptionalInt.of(scanParallel);
        resetTransactionOption();
        return self();
    }

    /**
     * Get scan parallel.
     *
     * @return scan parallel
     * @since 1.9.0
     */
    public synchronized OptionalInt scanParallel() {
        return this.scanParallel;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected synchronized void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        this.scanParallel.ifPresent(lowBuilder::setScanParallel);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected TgTxOptionRtx fillFrom(TgTxOption txOption) {
        super.fillFrom(txOption);

        if (txOption instanceof TgTxOptionRtx) {
            var src = (TgTxOptionRtx) txOption;
            this.scanParallel = src.scanParallel;
        }

        return self();
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public int hashCode() {
        return super.hashCode() ^ Objects.hash(scanParallel);
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (obj instanceof TgTxOptionRtx) {
            var that = (TgTxOptionRtx) obj;
            return Objects.equals(scanParallel(), that.scanParallel);
        }
        return false;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        this.scanParallel.ifPresent(n -> {
            appendString(sb, "scanParallel", n);
        });
    }
}
