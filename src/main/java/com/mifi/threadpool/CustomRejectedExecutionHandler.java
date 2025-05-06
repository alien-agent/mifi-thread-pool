package com.mifi.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

public class CustomRejectedExecutionHandler implements RejectedExecutionHandler {
    private static final Logger logger = LoggerFactory.getLogger(CustomRejectedExecutionHandler.class);

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        logger.warn("[Rejected] Task {} was rejected due to overload!", r);
        throw new RuntimeException("Task rejected due to thread pool overload");
    }
} 