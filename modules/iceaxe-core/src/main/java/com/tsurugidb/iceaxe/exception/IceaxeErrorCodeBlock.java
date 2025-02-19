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
package com.tsurugidb.iceaxe.exception;

/**
 * Iceaxe diagnostic code block.
 *
 * @see IceaxeErrorCode
 */
public final class IceaxeErrorCodeBlock {

    static final int COMMON = 0;
    static final int CONNECTOR = 1000;
    static final int SESSION = 2000;
    static final int TRANSACTION_MANAGER = 3000;
    static final int TRANSACTION = 4000;
    static final int STATEMENT = 5000;
    static final int RESULT = 6000;
    static final int EXPLAIN = 7000;
    static final int METADATA = 8000;
    static final int OBJECT = 9000;

    private IceaxeErrorCodeBlock() {
    }
}
