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
package com.tsurugidb.iceaxe.test.low;

import java.io.IOException;

import com.tsurugidb.tsubakuro.channel.common.connection.wire.Wire;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.ShutdownType;
import com.tsurugidb.tsubakuro.util.FutureResponse;
import com.tsurugidb.tsubakuro.util.ServerResource;

public class TestLowSession extends TestServerResource implements Session {

    @Override
    public void connect(Wire sessionWire) {
        throw new AssertionError("do override");
    }

    @Override
    public Wire getWire() {
        throw new AssertionError("do override");
    }

    @Override
    public void put(ServerResource resource) {
        throw new AssertionError("do override");
    }

    @Override
    public void remove(ServerResource resource) {
        throw new AssertionError("do override");
    }

    @Override
    public FutureResponse<Void> shutdown(ShutdownType type) throws IOException {
        return new TestFutureResponse<>();
    }
}
