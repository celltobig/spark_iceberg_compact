package dao;

public class IcebergTable {
    private Long Id;
    private String icebergDbTableName;
    private Long expireSnapshot;
    private Long removeOrphan;
    private String rewriteDataFiles;
    private String rewritePositionDeleteFiles;
    private String fullRewriteDataFilesTime;
    private Long fullRewrite;
    private Long status;

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public String getIcebergDbTableName() {
        return icebergDbTableName;
    }

    public void setIcebergDbTableName(String icebergDbTableName) {
        this.icebergDbTableName = icebergDbTableName;
    }

    public Long getExpireSnapshot() {
        return expireSnapshot;
    }

    public void setExpireSnapshot(Long expireSnapshot) {
        this.expireSnapshot = expireSnapshot;
    }

    public Long getRemoveOrphan() {
        return removeOrphan;
    }

    public void setRemoveOrphan(Long removeOrphan) {
        this.removeOrphan = removeOrphan;
    }

    public String getRewriteDataFiles() {
        return rewriteDataFiles;
    }

    public void setRewriteDataFiles(String rewriteDataFiles) {
        this.rewriteDataFiles = rewriteDataFiles;
    }

    public String getRewritePositionDeleteFiles() {
        return rewritePositionDeleteFiles;
    }

    public void setRewritePositionDeleteFiles(String rewritePositionDeleteFiles) {
        this.rewritePositionDeleteFiles = rewritePositionDeleteFiles;
    }

    public Long getStatus() {
        return status;
    }

    public void setStatus(Long status) {
        this.status = status;
    }

    public String getFullRewriteDataFilesTime() {
        return fullRewriteDataFilesTime;
    }

    public void setFullRewriteDataFilesTime(String fullRewriteDataFilesTime) {
        this.fullRewriteDataFilesTime = fullRewriteDataFilesTime;
    }

    public Long getFullRewrite() {
        return fullRewrite;
    }

    public void setFullRewrite(Long fullRewrite) {
        this.fullRewrite = fullRewrite;
    }
}
