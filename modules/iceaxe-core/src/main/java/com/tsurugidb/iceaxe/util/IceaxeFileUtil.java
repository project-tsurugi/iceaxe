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
package com.tsurugidb.iceaxe.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Objects;

/**
 * Iceaxe file utility.
 *
 * @since 1.8.0
 */
public final class IceaxeFileUtil {

    private IceaxeFileUtil() {
        // don't instantiate
    }

    /**
     * Write a {@linkplain java.lang.CharSequence CharSequence} to a file.
     *
     * @param path    the path to the file
     * @param csq     the CharSequence to be written
     * @param options options specifying how the file is opened
     * @return the path
     * @throws IOException if an I/O error occurs writing to or creating the file
     */
    public static Path writeString(Path path, CharSequence csq, OpenOption... options) throws IOException {
        Objects.requireNonNull(csq);

        byte[] bytes = csq.toString().getBytes(StandardCharsets.UTF_8);
        return Files.write(path, bytes, options);
    }
}
