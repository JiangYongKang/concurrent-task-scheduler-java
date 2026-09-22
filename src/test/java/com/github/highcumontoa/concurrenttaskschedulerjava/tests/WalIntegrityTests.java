package com.github.highcumontoa.concurrenttaskschedulerjava.tests;

import com.github.highcumontoa.concurrenttaskschedulerjava.config.SchedulerProperties;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskStatus;
import com.github.highcumontoa.concurrenttaskschedulerjava.store.WalTaskStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.*;

/** WAL 完整性：崩溃导致最后一行写一半时，恢复需截断撕裂尾部且保留此前完整快照。 */
class WalIntegrityTests {

    private static final Logger log = LoggerFactory.getLogger(WalIntegrityTests.class);

    @Test
    void tornTailIsTruncatedOnRecovery(@TempDir Path dir) throws Exception {
        Path wal = dir.resolve("torn.wal");
        SchedulerProperties props = new SchedulerProperties();
        props.setWalFile(wal.toString());
        props.setWalFsync(false);

        // 正常写入两个任务快照
        try (WalTaskStore store = new WalTaskStore(wal.toString(), false)) {
            store.append(record("a", TaskStatus.SUCCEEDED, "r-a"));
            store.append(record("b", TaskStatus.QUEUED, null));
        }
        // 模拟崩溃：追加半行 JSON
        Files.writeString(wal, "{\"taskId\":\"c\",\"status\":\"RU",
                StandardOpenOption.APPEND);
        long sizeBefore = Files.size(wal);

        try (WalTaskStore store = new WalTaskStore(wal.toString(), false)) {
            assertEquals(TaskStatus.SUCCEEDED, store.find("a").orElseThrow().getStatus());
            assertEquals("r-a", store.find("a").orElseThrow().getResult());
            assertEquals(TaskStatus.QUEUED, store.find("b").orElseThrow().getStatus());
            assertTrue(store.find("c").isEmpty(), "撕裂记录不得被恢复");
            assertEquals(2, store.loadAll().size());
            log.info("[WAL] 撕裂尾部恢复成功: a/b 完整, c 丢弃");

            // 截断后新追加必须落在干净文件末尾
            store.append(record("d", TaskStatus.SUCCEEDED, "r-d"));
        }
        long sizeAfter = Files.size(wal);
        assertTrue(sizeAfter < sizeBefore || Files.readString(wal).contains("r-d"));
        try (WalTaskStore store = new WalTaskStore(wal.toString(), false)) {
            assertEquals(3, store.loadAll().size());
            assertEquals("r-d", store.find("d").orElseThrow().getResult());
            log.info("[WAL] 截断后追加 d 并再次回放成功");
        }
    }

    private static TaskRecord record(String id, TaskStatus status, String result) {
        TaskRecord r = new TaskRecord();
        r.setTaskId(id);
        r.setCallerId("alice");
        r.setGroup("g");
        r.setTaskType("sample");
        r.setStatus(status);
        r.setAttempts(1);
        r.setResult(result);
        r.setVersion(1);
        return r;
    }
}
