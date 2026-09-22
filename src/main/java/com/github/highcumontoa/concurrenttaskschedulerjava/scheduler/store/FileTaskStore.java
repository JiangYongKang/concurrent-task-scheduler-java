package com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.highcumontoa.concurrenttaskschedulerjava.scheduler.domain.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地文件系统的任务存储。
 *
 * <ul>
 *   <li>每个任务一个文件 {@code <dataDir>/<taskId>.json}；</li>
 *   <li>写入采用「临时文件 + 原子 rename」，进程在任意时刻崩溃只会留下上一完整版本或新版本；</li>
 *   <li>(caller, submitKey) -> taskId 幂等索引：加载时重建，运行时由 {@link ConcurrentHashMap} 维护，
 *       并发重复提交只有一个 putIfAbsent 成功；</li>
 *   <li>submitKey 允许为空（空表示不去重），仅非空 key 进入索引。</li>
 * </ul>
 */
public class FileTaskStore implements TaskStore {

    private static final Logger log = LoggerFactory.getLogger(FileTaskStore.class);

    private final Path dataDir;
    private final ObjectMapper mapper;
    private final Map<String, String> submitKeyIndex = new ConcurrentHashMap<>();
    private final Object writeLock = new Object();

    public FileTaskStore(String dataDir) {
        this(Paths.get(dataDir), new ObjectMapper());
    }

    public FileTaskStore(Path dataDir, ObjectMapper mapper) {
        this.dataDir = dataDir;
        this.mapper = mapper;
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new TaskStoreException("无法创建任务数据目录: " + dataDir, e);
        }
        rebuildIndex();
    }

    private static String indexKey(String caller, String submitKey) {
        return caller + "::" + submitKey;
    }

    private void rebuildIndex() {
        List<TaskRecord> all = loadAll();
        for (TaskRecord r : all) {
            if (r.getSubmitKey() != null && !r.getSubmitKey().isEmpty()) {
                submitKeyIndex.putIfAbsent(indexKey(r.getCaller(), r.getSubmitKey()), r.getTaskId());
            }
        }
        log.info("任务存储初始化完成: dir={}, tasks={}, indexedKeys={}",
                dataDir, all.size(), submitKeyIndex.size());
    }

    private Path taskFile(String taskId) {
        return dataDir.resolve(taskId + ".json");
    }

    @Override
    public TaskRecord putIfAbsent(TaskRecord record) {
        String key = record.getSubmitKey();
        // 索引检查与文件写入必须在同一把锁内，杜绝「索引已发布、文件未写好」的窗口
        synchronized (writeLock) {
            if (key != null && !key.isEmpty()) {
                String existingId = submitKeyIndex.get(indexKey(record.getCaller(), key));
                if (existingId != null) {
                    TaskRecord existing = readFile(taskFile(existingId));
                    log.debug("重复提交命中索引: caller={}, submitKey={}, existingTaskId={}",
                            record.getCaller(), key, existingId);
                    return existing;
                }
            }
            Path target = taskFile(record.getTaskId());
            if (Files.exists(target)) {
                return readFile(target);
            }
            writeAtomic(target, record);
            if (key != null && !key.isEmpty()) {
                submitKeyIndex.put(indexKey(record.getCaller(), key), record.getTaskId());
            }
            return null;
        }
    }

    @Override
    public void save(TaskRecord record) {
        synchronized (writeLock) {
            writeAtomic(taskFile(record.getTaskId()), record);
        }
    }

    @Override
    public TaskRecord findById(String taskId) {
        Path f = taskFile(taskId);
        if (!Files.exists(f)) {
            return null;
        }
        return readFile(f);
    }

    @Override
    public TaskRecord findBySubmitKey(String caller, String submitKey) {
        if (submitKey == null || submitKey.isEmpty()) {
            return null;
        }
        String id = submitKeyIndex.get(indexKey(caller, submitKey));
        return id == null ? null : findById(id);
    }

    @Override
    public List<TaskRecord> loadAll() {
        List<TaskRecord> result = new ArrayList<>();
        if (!Files.isDirectory(dataDir)) {
            return result;
        }
        try (var stream = Files.list(dataDir)) {
            List<Path> files = stream
                    .filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted()
                    .toList();
            for (Path f : files) {
                try {
                    result.add(readFile(f));
                } catch (TaskStoreException e) {
                    log.warn("跳过无法解析的任务文件: {} ({})", f, e.getMessage());
                }
            }
        } catch (IOException e) {
            throw new TaskStoreException("加载任务目录失败: " + dataDir, e);
        }
        return result;
    }

    private void writeAtomic(Path target, TaskRecord record) {
        Path tmp = target.resolveSibling(
                target.getFileName() + ".tmp-" + Thread.currentThread().threadId());
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(record);
            Files.write(tmp, bytes);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicUnsupported) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new TaskStoreException("任务写盘失败: taskId=" + record.getTaskId(), e);
        } finally {
            try {
                Files.deleteIfExists(tmp);
            } catch (IOException ignored) {
                // 临时文件残留不影响正确性（loadAll 只读取 .json）
            }
        }
    }

    private TaskRecord readFile(Path f) {
        try {
            byte[] bytes = Files.readAllBytes(f);
            return mapper.readValue(new String(bytes, StandardCharsets.UTF_8), TaskRecord.class);
        } catch (IOException e) {
            throw new TaskStoreException("任务文件读取失败: " + f, e);
        }
    }
}
