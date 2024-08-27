/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.iceaxe.example;

import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiDefaultRetryPredicate;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiTmRetryPredicate;

/**
 * {@link TsurugiTmRetryPredicate} example.
 *
 * @see Example04TmSetting
 */
public class Example04TmRetryPredicate {

    void predicate(TgTmSetting setting) {
        var supplier = setting.getTransactionOptionSupplier();
        supplier.setRetryPredicate(new TsurugiDefaultRetryPredicate() {
            @Override
            protected boolean isRetryable(TsurugiTransactionException exception) {
                var exceptionUtil = TsurugiExceptionUtil.getInstance();
                if (exceptionUtil.isSerializationFailure(exception)) {
                    return true;
                }
                return super.isRetryable(exception);
            }
        });
    }
}
