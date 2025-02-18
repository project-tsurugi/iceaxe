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
package com.tsurugidb.iceaxe.transaction.manager.retry;

/**
 * Tsurugi TransactionManager retry code.
 */
public enum TgTmRetryStandardCode implements TgTmRetryCode {

    /** not retryable */
    NOT_RETRYABLE(false),

    /** retryable. by serialization failure */
    RETRYABLE(true),
    /** retryable to LTX. by conflict on write preserve */
    RETRYABLE_LTX(true),

    ;

    private final boolean retryable;

    private TgTmRetryStandardCode(boolean retryable) {
        this.retryable = retryable;
    }

    @Override
    public boolean isRetryable() {
        return this.retryable;
    }
}
