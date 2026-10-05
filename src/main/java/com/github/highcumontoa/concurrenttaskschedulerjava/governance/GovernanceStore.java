package com.github.highcumontoa.concurrenttaskschedulerjava.governance;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
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

/**
 * 治理状态（运行期配额覆盖 + 暂停标记）的本地持久化存储。
 *
 * <p>与任务 WAL 分离为独立文件，避免改动既有任务持久化语义；
 * 追加式 JSON Lines，同一维度以最新一行为准回放，
 * 因此运行期的配额调整与暂停/恢复状态都能跨进程重启保留。
 *
 * <p>崩溃时最后一行可能写了一半：加载阶段遇到无法解析的行即视为尾部撕裂，
 * 截断该偏移之后的内容后继续，与任务 WAL 的恢复策略一致。
 */
public class GovernanceStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GovernanceStore.class);

    private final Path path;
    private final boolean fsyncEachWrite;
    private final ObjectMapper mapper;
    private final Map<GovernanceScope, GovernanceRecord> index = new LinkedHashMap<>();
    private FileOutputStream fos;

    public GovernanceStore(String filePath, boolean fsyncEachWrite) {
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
            throw new UncheckedIOException("failed to open governance WAL: " + filePath, e);
        }
    }

    /** 从治理 WAL 回放，并截断尾部撕裂内容。 */
    private synchronized void recover() throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        int lineStart = 0;
        int validEnd = 0;
        for (int i = 0; i <= bytes.length; i++) {
            if (i == bytes.length || bytes[i] == '\n') {
                int len = i - lineStart;
                if (len > 0) {
                    String line = new String(bytes, lineStart, len, StandardCharsets.UTF_8);
                    GovernanceRecord record = null;
                    try {
                        record = mapper.readValue(line, GovernanceRecord.class);
                    } catch (IOException parseError) {
                        log.warn("治理 WAL 撕裂/损坏行，将从偏移 {} 截断: {}", lineStart,
                                parseError.getMessage());
                    }
                    if (record == null || !hasScope(record)) {
                        break;
                    }
                    index.put(keyOf(record), record);
                }
                validEnd = i < bytes.length ? i + 1 : bytes.length;
                lineStart = i + 1;
                if (i == bytes.length) {
                    break;
                }
            }
        }
        if (validEnd < bytes.length) {
            try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(path,
                    java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(validEnd);
            }
            log.warn("治理 WAL 已截断损坏尾部: {} -> {} 字节", bytes.length, validEnd);
        }
        log.info("治理 WAL 恢复完成: {} 个维度, 文件={}", index.size(), path);
    }

    private static GovernanceScope keyOf(GovernanceRecord r) {
        return GovernanceScope.of(r.getCallerId(), r.getGroup());
    }

    /** 作用域信息完整：callerId 与 group 至少一个非空白。 */
    private static boolean hasScope(GovernanceRecord r) {
        return (r.getCallerId() != null && !r.getCallerId().isBlank())
                || (r.getGroup() != null && !r.getGroup().isBlank());
    }

    /** 追加一条治理状态快照（返回后即视为已落盘）。 */
    public synchronized void append(GovernanceRecord record) {
        if (!hasScope(record)) {
            throw new IllegalArgumentException(
                    "callerId and group must not both be absent (missing governance scope)");
        }
        try {
            fos.write(mapper.writeValueAsBytes(record));
            fos.write('\n');
            fos.flush();
            if (fsyncEachWrite) {
                fos.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "failed to append governance WAL for " + keyOf(record), e);
        }
        index.put(keyOf(record), record);
    }

    /** 全部维度的最新治理状态（按首次出现顺序）。 */
    public synchronized List<GovernanceRecord> loadAll() {
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
            throw new UncheckedIOException("failed to close governance WAL", e);
        }
    }
}
