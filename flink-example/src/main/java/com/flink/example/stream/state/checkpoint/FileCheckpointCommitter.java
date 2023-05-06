package com.flink.example.stream.state.checkpoint;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.runtime.operators.CheckpointCommitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * 功能：FileCheckpointCommitter
 *      将检查点提交到文件中
 * 作者：SmartSi
 * CSDN博客：https://blog.csdn.net/sunnyyoona
 * 公众号：大数据生态
 * 日期：2022/8/19 下午11:32
 */
public class FileCheckpointCommitter extends CheckpointCommitter {

    private static final Logger LOG = LoggerFactory.getLogger(FileCheckpointCommitter.class);

    private String jobBasePath;
    private final String basePath;

    public FileCheckpointCommitter(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public void open() throws Exception {
        LOG.info("open committer");
        // no need to open a connection
    }

    @Override
    public void close() throws Exception {
        LOG.info("close committer");
        // no need to close a connection
    }

    // 创建资源(在这为文件)
    @Override
    public void createResource() throws Exception {
        this.jobBasePath = this.basePath + "/" + this.jobId;
        // 当前 JobId 作为提交文件的目录
        Files.createDirectory(Paths.get(this.jobBasePath));
        LOG.info("create resource {}", this.jobBasePath);
    }

    // 提交 Checkpoint(为每个任务实例提交)
    @Override
    public void commitCheckpoint(int subTaskIdx, long checkpointID) throws Exception {
        Path commitPath = Paths.get(this.jobBasePath + "/" + subTaskIdx);
        // 将 CheckpointID 转换为 16 进制字符串
        String hexID = "0x" + StringUtils.leftPad(Long.toHexString(checkpointID), 16, "0");
        // 将 16 进制字符串写进提交文件中
        Files.write(commitPath, hexID.getBytes());
        LOG.info("CheckpointId {} (SubTask = {}) commit, path is {}", checkpointID, subTaskIdx, commitPath);
    }

    // 判断该子任务对应的 Checkpoint 是否已经提交
    @Override
    public boolean isCheckpointCommitted(int subTaskIdx, long checkpointID) throws Exception {
        boolean isCommitted;
        Path commitPath = Paths.get(this.jobBasePath + "/" + subTaskIdx);
        if (!Files.exists(commitPath)) {
            // 提交文件都没有表示没有提交过
            isCommitted = false;
        } else {
            // 从文件中读取提交的 CheckpointId
            String hexID = Files.readAllLines(commitPath).get(0);
            Long commitCheckpointID = Long.decode(hexID);
            // 判断当前 CheckpointID 是否小于等于已提交的 CheckpointID
            isCommitted = checkpointID <= commitCheckpointID;
        }
        if (isCommitted) {
            LOG.info("CheckpointId {} (SubTask = {}) is committed", checkpointID, subTaskIdx);
        } else {
            LOG.info("CheckpointId {} (SubTask = {}) has not committed", checkpointID, subTaskIdx);
        }
        return isCommitted;
    }
}
