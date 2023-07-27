package com.tsurugidb.iceaxe.transaction;

import com.tsurugidb.iceaxe.util.IceaxeInternal;
import com.tsurugidb.sql.proto.SqlRequest.CommitStatus;

/**
 * Tsurugi Commit Type
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
     * get {@link CommitStatus}
     *
     * @return commit status
     */
    @IceaxeInternal
    public CommitStatus getLowCommitStatus() {
        return this.lowCommitStatus;
    }
}
