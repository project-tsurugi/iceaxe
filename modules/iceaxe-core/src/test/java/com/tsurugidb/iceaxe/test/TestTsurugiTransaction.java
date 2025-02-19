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
package com.tsurugidb.iceaxe.test;

import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class TestTsurugiTransaction extends TsurugiTransaction {

    public static TestTsurugiTransaction of() {
        var session = new TestTsurugiSession(TgSessionOption.of());
        var txOption = TgTxOption.ofOCC();
        return new TestTsurugiTransaction(session, txOption);
    }

    public TestTsurugiTransaction(TsurugiSession session, TgTxOption transactionOption) {
        super(session, transactionOption);
    }
}
