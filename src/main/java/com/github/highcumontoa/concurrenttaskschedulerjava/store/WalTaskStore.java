package com.github.highcumontoa.concurrenttaskschedulerjava.store;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.concurrenttaskschedulerjava.model.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 基于本地文件的 JSON Lines WAL 存储。
 *
 * <p>每条状态快照序列化为一行 JSON 追加写入；同一任务以最新一行为准回放，
 * 从而在进程重启后恢复最新状态（不丢任务、不回退）。
 *
 * <p>崩溃时最后一行可能写了一半：加载阶段遇到首行无法解析即视为尾部撕裂，
 * 截断该字节偏移之后的全部内容后继续，保证后续追加不破坏日志。
 *
 * <p>内存中以 LinkedHashMap 保留入队顺序，供调度器做近似 FIFO 派发。
 */
public class WalTaskStore implements TaskStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WalTaskStore.class);

    private final Path path;
    private final boolean fsyncEachWrite;
    private final ObjectMapper mapper;
    private final Map<String, TaskRecord> index = new LinkedHashMap<>();
    private FileOutputStream fos;

    public WalTaskStore(String filePath, boolean fsyncEachWrite) {
        this.path = Path.of(filePath);
        this.fsyncEachWrite = fsyncEachWrite;
        this.mapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        try {
            File parent = path.toFile().getParentFile();
            if (parent != null) {
                parent.mkdirs();
            }
            if (!Files.exists(path)) {
                Files.createFile(path);
            }
            recover();
            this.fos = new FileOutputStream(path.toFile(), true);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to open WAL: " + filePath, e);
        }
    }

    /** 从 WAL 回放，并截断尾部撕裂内容。 */
    private synchronized void recover() throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int lineStart = 0;
        int validEnd = 0;
        for (int i = 0; i <= bytes.length; i++) {
            if (i == bytes.length || bytes[i] == '\n') {
                int len = i - lineStart;
                if (len > 0) {
                    String line = new String(bytes, lineStart, len, StandardCharsets.UTF_8);
                    TaskRecord record = null;
                    try {
                        record = mapper.readValue(line, TaskRecord.class);
                    } catch (IOException parseError) {
                        log.warn("WAL 撕裂/损坏行，将从偏移 {} 截断: {}", lineStart,
                                parseError.getMessage());
                    }
                    if (record == null || record.getTaskId() == null) {
                        break;
                    }
                    index.put(record.getTaskId(), record);
                }
                validEnd = i < bytes.length ? i + 1 : bytes.length;
                lineStart = i + 1;
                if (i == bytes.length) {
                    break;
                }
            }
        }
        if (validEnd < bytes.length) {
            try (FileChannelHolder ch = new FileChannelHolder(path)) {
                ch.channel.truncate(validEnd);
            }
            log.warn("WAL 已截断损坏尾部: {} -> {} 字节", bytes.length, validEnd);
        }
        log.info("WAL 恢复完成: {} 个任务, 文件={}", index.size(), path);
    }

    @Override
    public synchronized void append(TaskRecord record) {
        if (record.getTaskId() == null) {
            throw new IllegalArgumentException("taskId must not be null");
        }
        try {
            fos.write(mapper.writeValueAsBytes(record));
            fos.write('\n');
            fos.flush();
            if (fsyncEachWrite) {
                fos.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append WAL for task " + record.getTaskId(), e);
        }
        index.put(record.getTaskId(), record);
    }

    @Override
    public synchronized Optional<TaskRecord> find(String taskId) {
        return Optional.ofNullable(index.get(taskId));
    }

    @Override
    public synchronized List<TaskRecord> loadAll() {
        return new ArrayList<>(index.values());
    }

    @Override
    public synchronized void close() {
        try {
            if (fos != null) {
                fos.flush();
                fos.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close WAL", e);
        }
    }

    private static final class FileChannelHolder implements AutoCloseable {
        private final java.nio.channels.FileChannel channel;

        private FileChannelHolder(Path path) throws IOException {
            this.channel = java.nio.channels.FileChannel.open(path,
                    java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE);
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
