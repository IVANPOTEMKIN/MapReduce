package ru.effective_mobile.service;

import lombok.RequiredArgsConstructor;
import ru.effective_mobile.model.KeyValue;
import ru.effective_mobile.model.Task;

import java.util.*;
import java.util.logging.Logger;

@RequiredArgsConstructor
public class Worker implements Runnable {

    private static final Logger logger = Logger.getLogger(Worker.class.getName());
    private final Coordinator coordinator;

    @Override
    public void run() {
        try {
            logger.info("Worker started.\n");
            while (true) {
                Task task = coordinator.getTask();

                if (task == null) {
                    logger.info("No more tasks. Worker is shutting down.\n");
                    break;
                }

                if (task.getFileName() != null) {
                    logger.info("Starting Map task for file: " + task.getFileName() + " (Task ID: " + task.getId() + ")\n");
                    executeMap(task);
                    logger.info("Map task for file: " + task.getFileName() + " (Task ID: " + task.getId() + ") completed.\n");

                } else {
                    logger.info("Starting Reduce task for ID: " + task.getId() + "\n");
                    executeReduce(task.getId());
                    logger.info("Reduce task for ID: " + task.getId() + " completed.\n");
                }
            }
        } catch (Exception e) {
            logger.warning("Exception in Worker thread.\n");
            throw new RuntimeException("Worker encountered an error", e);
        }
    }

    private void executeMap(Task task) {
        String content = FileHandler.readFile(task.getFileName());
        List<KeyValue> list = map(content);
        Map<Integer, List<KeyValue>> map = distribute(list, task.getCountReduceTask());
        FileHandler.writeTempFile(task.getId(), map);
        coordinator.finish();
    }

    private void executeReduce(int reduceTaskId) {
        List<KeyValue> list = new ArrayList<>();
        for (int i = 0; i < coordinator.getCountMapTask(); i++) {
            String fileName = String.format(FileHandler.TEMP_FILE_FORMAT, i, reduceTaskId);
            list.addAll(FileHandler.readTempFile(fileName));
        }
        list.sort(Comparator.comparing(KeyValue::getKey));
        Map<String, List<String>> map = new HashMap<>();
        for (KeyValue element : list) {
            map.computeIfAbsent(element.getKey(), k -> new ArrayList<>()).add(element.getValue());
        }
        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String result = reduce(entry.getValue());
            FileHandler.writeResultFile(entry.getKey(), result);
        }
        coordinator.finish();
    }

    private Map<Integer, List<KeyValue>> distribute(List<KeyValue> list, int countReduceTask) {
        Map<Integer, List<KeyValue>> map = new HashMap<>();
        for (KeyValue element : list) {
            int idx = Math.abs(element.getKey().hashCode()) % countReduceTask;
            map.computeIfAbsent(idx, k -> new ArrayList<>()).add(element);
        }
        return map;
    }

    public List<KeyValue> map(String content) {
        List<KeyValue> list = new ArrayList<>();
        String[] words = content.split("\\W+");
        for (String word : words) {
            list.add(new KeyValue(word, "1"));
        }
        return list;
    }

    public String reduce(List<String> values) {
        int sum = values.stream().mapToInt(Integer::parseInt).sum();
        return String.valueOf(sum);
    }
}