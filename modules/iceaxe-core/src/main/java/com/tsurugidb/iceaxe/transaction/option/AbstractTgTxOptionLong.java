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

import javax.annotation.OverridingMethodsMustInvokeSuper;
import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionOption;
import com.tsurugidb.sql.proto.SqlRequest.TransactionPriority;

/**
 * Tsurugi Transaction Option (long transaction).
 *
 * @param <T> concrete class
 */
@ThreadSafe
public abstract class AbstractTgTxOptionLong<T extends AbstractTgTxOptionLong<T>> extends AbstractTgTxOption<T> {

    private TransactionPriority lowPriority = null;

    /**
     * <em>This method is not yet implemented:</em>
     * set priority.
     *
     * @param priority priority
     * @return this
     */
    public synchronized T priority(TransactionPriority priority) {
        this.lowPriority = priority;
        resetTransactionOption();
        return self();
    }

    /**
     * get priority.
     *
     * @return priority
     */
    public synchronized TransactionPriority priority() {
        return this.lowPriority;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected synchronized void initializeLowTransactionOption(TransactionOption.Builder lowBuilder) {
        super.initializeLowTransactionOption(lowBuilder);

        if (this.lowPriority != null) {
            lowBuilder.setPriority(lowPriority);
        }
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected T fillFrom(TgTxOption txOption) {
        super.fillFrom(txOption);

        if (txOption instanceof AbstractTgTxOptionLong) {
            var src = (AbstractTgTxOptionLong<?>) txOption;
            priority(src.priority());
        }

        return self();
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public int hashCode() {
        return super.hashCode() ^ Objects.hash(priority());
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        if (obj instanceof AbstractTgTxOptionLong) {
            var that = (AbstractTgTxOptionLong<?>) obj;
            return Objects.equals(priority(), that.priority());
        }
        return false;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    protected void toString(StringBuilder sb) {
        super.toString(sb);

        if (this.lowPriority != null) {
            appendString(sb, "priority", getTransactionPriorityName(lowPriority));
        }
    }

    private static String getTransactionPriorityName(TransactionPriority lowPriority) {
        switch (lowPriority) {
        case TRANSACTION_PRIORITY_UNSPECIFIED:
            return "DEFAULT";
        default:
            return lowPriority.name();
        }
    }
}
