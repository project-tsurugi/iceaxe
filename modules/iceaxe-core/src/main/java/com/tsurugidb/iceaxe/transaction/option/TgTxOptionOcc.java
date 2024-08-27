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
package com.tsurugidb.iceaxe.transaction.option;

import javax.annotation.concurrent.ThreadSafe;

import com.tsurugidb.sql.proto.SqlRequest.TransactionType;

/**
 * Tsurugi Transaction Option (OCC).
 */
@ThreadSafe
public class TgTxOptionOcc extends AbstractTgTxOption<TgTxOptionOcc> {

    @Override
    public String typeName() {
        return "OCC";
    }

    @Override
    public TransactionType type() {
        return TransactionType.SHORT;
    }
}
