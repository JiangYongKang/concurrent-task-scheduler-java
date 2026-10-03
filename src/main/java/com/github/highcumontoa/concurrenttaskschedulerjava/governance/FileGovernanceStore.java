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
import java.util.List;

/**
 * 基于本地文件的治理事件存储（JSON Lines）。
 *
 * <p>与任务 WAL 采用相同的可靠性约定：每次变更追加一行、可选 fsync；
 * 打开时顺序回放全部事件，崩溃导致的尾部撕裂行会被截断，此前完整记录不受影响。
 * 调度器启动时按顺序回放，每个 {@link GovernanceScope} 以最后一条事件为准。
 */
public class FileGovernanceStore implements GovernanceStore, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(FileGovernanceStore.class);

    private final Path path;
    private final boolean fsyncEachWrite;
    private final ObjectMapper mapper;
    private FileOutputStream fos;

    public FileGovernanceStore(String filePath, boolean fsyncEachWrite) {
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
            throw new UncheckedIOException("failed to open governance log: " + filePath, e);
        }
    }

    /** 顺序读回事件，并截断无法解析的撕裂尾部。 */
    private synchronized List<GovernanceEvent> recover() throws IOException {
        byte[] bytes = Files.readAllBytes(path);
        List<GovernanceEvent> events = new ArrayList<>();
        int lineStart = 0;
        int validEnd = 0;
        for (int i = 0; i <= bytes.length; i++) {
            if (i == bytes.length || bytes[i] == '\n') {
                int len = i - lineStart;
                if (len > 0) {
                    String line = new String(bytes, lineStart, len, StandardCharsets.UTF_8);
                    GovernanceEvent event = null;
                    try {
                        event = mapper.readValue(line, GovernanceEvent.class);
                    } catch (IOException parseError) {
                        log.warn("治理日志撕裂/损坏行，将从偏移 {} 截断: {}", lineStart,
                                parseError.getMessage());
                    }
                    if (event == null || event.getKind() == null) {
                        break;
                    }
                    events.add(event);
                }
                validEnd = i < bytes.length ? i + 1 : bytes.length;
                lineStart = i + 1;
                if (i == bytes.length) {
                    break;
                }
            }
        }
        if (validEnd < bytes.length) {
            try (var ch = java.nio.channels.FileChannel.open(path,
                    java.nio.file.StandardOpenOption.READ,
                    java.nio.file.StandardOpenOption.WRITE)) {
                ch.truncate(validEnd);
            }
            log.warn("治理日志已截断损坏尾部: {} -> {} 字节", bytes.length, validEnd);
        }
        log.info("治理日志恢复完成: {} 条事件, 文件={}", events.size(), path);
        return events;
    }

    @Override
    public synchronized void append(GovernanceEvent event) {
        if (event == null || event.getKind() == null) {
            throw new IllegalArgumentException("governance event and kind must not be null");
        }
        try {
            fos.write(mapper.writeValueAsBytes(event));
            fos.write('\n');
            fos.flush();
            if (fsyncEachWrite) {
                fos.getFD().sync();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to append governance event", e);
        }
    }

    @Override
    public synchronized List<GovernanceEvent> loadAll() {
        try {
            byte[] bytes = Files.readAllBytes(path);
            List<GovernanceEvent> events = new ArrayList<>();
            int lineStart = 0;
            for (int i = 0; i <= bytes.length; i++) {
                if (i == bytes.length || bytes[i] == '\n') {
                    int len = i - lineStart;
                    if (len > 0) {
                        String line = new String(bytes, lineStart, len, StandardCharsets.UTF_8);
                        try {
                            GovernanceEvent event = mapper.readValue(line, GovernanceEvent.class);
                            if (event.getKind() != null) {
                                events.add(event);
                            }
                        } catch (IOException ignored) {
                            // 运行期读取：撕裂尾部按已完整行数返回即可
                            break;
                        }
                    }
                    lineStart = i + 1;
                    if (i == bytes.length) {
                        break;
                    }
                }
            }
            return events;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read governance log", e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (fos != null) {
                fos.flush();
                fos.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to close governance log", e);
        }
    }
}
