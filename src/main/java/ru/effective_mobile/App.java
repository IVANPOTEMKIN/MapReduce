package ru.effective_mobile;

import ru.effective_mobile.service.Coordinator;
import ru.effective_mobile.service.Worker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class App {

    private static final Logger logger = Logger.getLogger(App.class.getName());

    public static void main(String[] args) {
        String[] fileNames = {"file1.txt", "file2.txt", "file3.txt"};

        int countMapTask = fileNames.length;
        int countReduceTask = 2;
        int countWorkers = 2;

        Coordinator coordinator = Coordinator.getInstance(countMapTask, countReduceTask, countWorkers);
        coordinator.addMapTasks(fileNames);

        ExecutorService workers = coordinator.getWorkers();
        for (int i = 0; i < countWorkers; i++) {
            workers.execute(new Worker(coordinator));
        }

        workers.shutdown();

        try {
            workers.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (coordinator.checkAllTasksCompleted()) {
            logger.info("All tasks completed.\n");
        } else {
            logger.warning("Any tasks were not completed.\n");
        }
    }
}