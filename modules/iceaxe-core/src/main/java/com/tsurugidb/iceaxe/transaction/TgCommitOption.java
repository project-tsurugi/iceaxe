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
package com.tsurugidb.iceaxe.transaction;

import java.util.Objects;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.CommitOption;

/**
 * Tsurugi Commit Option.
 *
 * @since X.X.X
 */
public class TgCommitOption {

    /**
     * Creates a new default commit option.
     *
     * @return commit option
     */
    public static TgCommitOption of() {
        return new TgCommitOption(TgCommitType.DEFAULT);
    }

    /**
     * Creates a new commit option.
     *
     * @param commitType commit type
     * @return commit option
     */
    public static TgCommitOption of(TgCommitType commitType) {
        return new TgCommitOption(commitType);
    }

    private TgCommitType commitType;
    private boolean autoDispose = false;

    /**
     * Creates a new instance.
     *
     * @param commitType commit type
     */
    @IceaxeInternal
    public TgCommitOption(TgCommitType commitType) {
        this.commitType = Objects.requireNonNull(commitType);
    }

    /**
     * Set commit type.
     *
     * @param commitType commit type
     */
    public void setCommitType(TgCommitType commitType) {
        this.commitType = Objects.requireNonNull(commitType);
    }

    /**
     * Get commit type.
     *
     * @return commit type
     */
    public TgCommitType getCommitType() {
        return this.commitType;
    }

    /**
     * Set auto dispose.
     *
     * @param autoDispose auto dispose
     */
    public void setAutoDispose(boolean autoDispose) {
        this.autoDispose = autoDispose;
    }

    /**
     * Get auto dispose.
     *
     * @return auto dispose
     */
    public boolean isAutoDispose() {
        return this.autoDispose;
    }

    /**
     * Get low commit option.
     *
     * @return low commit option
     */
    @IceaxeInternal
    public CommitOption toLowCommitOption() {
        var builder = CommitOption.newBuilder();
        builder.setNotificationType(commitType.getLowCommitStatus());
        builder.setAutoDispose(autoDispose);
        return builder.build();
    }

    @Override
    public String toString() {
        return "TgCommitOption{commitType=" + commitType + ", autoDispose=" + autoDispose + "}";
    }
}
