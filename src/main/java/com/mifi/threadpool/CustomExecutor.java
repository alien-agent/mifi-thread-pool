package com.mifi.threadpool;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface CustomExecutor extends ExecutorService {
    // Все методы уже определены в ExecutorService
} 