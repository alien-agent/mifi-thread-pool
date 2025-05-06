package com.mifi.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // Create thread pool with parameters
        CustomThreadPool pool = new CustomThreadPool(
            2,  // corePoolSize
            4,  // maxPoolSize
            5,  // keepAliveTime
            TimeUnit.SECONDS,  // timeUnit
            5,  // queueSize
            1   // minSpareThreads
        );

        // Submit some tasks
        for (int i = 0; i < 10; i++) {
            final int taskId = i;
            pool.execute(() -> {
                try {
                    logger.info("Task {} started", taskId);
                    Thread.sleep(1000); // Simulate work
                    logger.info("Task {} completed", taskId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("Task {} interrupted", taskId);
                }
            });
        }

        // Submit a task that returns a value
        pool.submit(() -> {
            logger.info("Callable task started");
            Thread.sleep(2000);
            logger.info("Callable task completed");
            return "Task result";
        });

        // Wait for some time to see the tasks being processed
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Shutdown the pool
        logger.info("Shutting down the pool...");
        pool.shutdown();

        // Try to submit a task after shutdown
        try {
            pool.execute(() -> logger.info("This task should be rejected"));
        } catch (Exception e) {
            logger.error("Expected rejection: {}", e.getMessage());
        }
    }
} 