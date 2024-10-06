package ru.effective_mobile.service;

import lombok.Data;
import ru.effective_mobile.model.Task;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

@Data
public class Coordinator {

    private static final Logger logger = Logger.getLogger(Coordinator.class.getName());
    private static volatile Coordinator instance;
    private final BlockingQueue<Task> tasks;
    private final ExecutorService workers;
    private final int countMapTask;
    private final int countReduceTask;
    private int completedTasks;

    private Coordinator(int countMapTask, int countReduceTask, int countWorkers) {
        this.tasks = new LinkedBlockingQueue<>();
        this.workers = Executors.newFixedThreadPool(countWorkers);
        this.countMapTask = countMapTask;
        this.countReduceTask = countReduceTask;
        this.completedTasks = 0;
    }

    public static Coordinator getInstance(int countMapTask, int countReduceTask, int countWorkers) {
        if (instance == null) {
            synchronized (Coordinator.class) {
                if (instance == null) {
                    instance = new Coordinator(countMapTask, countReduceTask, countWorkers);
                }
            }
        }
        return instance;
    }

    public void addMapTasks(String[] fileNames) {
        int taskId = 0;
        for (String fileName : fileNames) {
            tasks.add(new Task(taskId++, fileName, countReduceTask));
        }
    }

    public void addReduceTasks() {
        for (int i = 0; i < countReduceTask; i++) {
            tasks.add(new Task(i, null, 0));
        }
    }

    public Task getTask() {
        return tasks.poll();
    }

    public synchronized void finish() {
        completedTasks++;
        if (completedTasks == countMapTask) {
            logger.info("All map tasks completed.\n");
            addReduceTasks();
        }
        if (checkAllTasksCompleted()) {
            logger.info("All map and reduce tasks completed.\n");
        }
    }

    public boolean checkAllTasksCompleted() {
        return completedTasks == countMapTask + countReduceTask;
    }
}