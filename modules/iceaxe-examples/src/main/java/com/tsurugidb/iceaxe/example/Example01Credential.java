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
package com.tsurugidb.iceaxe.example;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;

import com.tsurugidb.tsubakuro.channel.common.connection.Credential;
import com.tsurugidb.tsubakuro.channel.common.connection.FileCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.NullCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.RememberMeCredential;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

/**
 * Credential example
 */
public class Example01Credential {

    public static Credential getCredential() {
        return getNullCredential();
    }

    static Credential getNullCredential() {
        return NullCredential.INSTANCE;
    }

    static Credential getUserPasswordCredential() {
        return new UsernamePasswordCredential("user", "password");
    }

    static Credential getTokenCredential() {
        return new RememberMeCredential("token");
    }

    static Credential getFileCredential() {
        var file = Path.of("/path/to/credential-file");
        try {
            return FileCredential.load(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
