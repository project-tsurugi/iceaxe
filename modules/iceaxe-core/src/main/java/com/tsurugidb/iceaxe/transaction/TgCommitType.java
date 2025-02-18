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

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;

/**
 * Tsurugi Commit Type.
 * <p>
 * This commitType determines when the {@link TsurugiTransaction#commit(TgCommitType)} returns.
 * </p>
 */
public enum TgCommitType {

    /**
     * rely on the database settings
     */
    DEFAULT(CommitStatus.COMMIT_STATUS_UNSPECIFIED),

    /**
     * commit operation has accepted (the transaction will never abort except system errors)
     */
    ACCEPTED(CommitStatus.ACCEPTED),

    /**
     * commit data has been visible for others
     */
    AVAILABLE(CommitStatus.AVAILABLE),

    /**
     * commit data has been saved on the local disk
     */
    STORED(CommitStatus.STORED),

    /**
     * commit data has been propagated to the all suitable nodes
     */
    PROPAGATED(CommitStatus.PROPAGATED),

    //
    ;

    private final CommitStatus lowCommitStatus;

    private TgCommitType(CommitStatus lowCommitStatus) {
        this.lowCommitStatus = lowCommitStatus;
    }

    /**
     * get {@link CommitStatus}.
     *
     * @return commit status
     */
    @IceaxeInternal
    public CommitStatus getLowCommitStatus() {
        return this.lowCommitStatus;
    }
}
