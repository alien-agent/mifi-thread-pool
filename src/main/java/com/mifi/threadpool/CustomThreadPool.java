package com.mifi.threadpool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class CustomThreadPool implements CustomExecutor {
    private static final Logger logger = LoggerFactory.getLogger(CustomThreadPool.class);
    
    private final int corePoolSize;
    private final int maxPoolSize;
    private final long keepAliveTime;
    private final TimeUnit timeUnit;
    private final int queueSize;
    private final int minSpareThreads;
    
    private final List<BlockingQueue<Runnable>> queues;
    private final List<Worker> workers;
    private final AtomicInteger activeThreads = new AtomicInteger(0);
    private final ReentrantLock mainLock = new ReentrantLock();
    private volatile boolean isShutdown = false;
    private volatile boolean isTerminated = false;
    
    private final CustomThreadFactory threadFactory;
    private final CustomRejectedExecutionHandler rejectionHandler;
    
    public CustomThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime, 
                          TimeUnit timeUnit, int queueSize, int minSpareThreads) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.timeUnit = timeUnit;
        this.queueSize = queueSize;
        this.minSpareThreads = minSpareThreads;
        
        this.queues = new ArrayList<>();
        this.workers = new ArrayList<>();
        this.threadFactory = new CustomThreadFactory("CustomPool");
        this.rejectionHandler = new CustomRejectedExecutionHandler();
        
        // Initialize core threads
        for (int i = 0; i < corePoolSize; i++) {
            addWorker();
        }
    }
    
    private void addWorker() {
        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(queueSize);
        queues.add(queue);
        Worker worker = new Worker(queue);
        workers.add(worker);
        worker.thread.start();
    }
    
    @Override
    public void execute(Runnable command) {
        if (isShutdown) {
            rejectionHandler.rejectedExecution(command, null);
            return;
        }
        
        // Try to find the least loaded queue
        BlockingQueue<Runnable> selectedQueue = null;
        int minSize = Integer.MAX_VALUE;
        
        for (BlockingQueue<Runnable> queue : queues) {
            int size = queue.size();
            if (size < minSize) {
                minSize = size;
                selectedQueue = queue;
            }
        }
        
        if (selectedQueue != null) {
            try {
                if (!selectedQueue.offer(command)) {
                    // Queue is full, try to create new worker if possible
                    if (activeThreads.get() < maxPoolSize) {
                        addWorker();
                        queues.get(queues.size() - 1).offer(command);
                    } else {
                        rejectionHandler.rejectedExecution(command, null);
                    }
                }
            } catch (Exception e) {
                rejectionHandler.rejectedExecution(command, null);
            }
        }
    }
    
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        FutureTask<T> futureTask = new FutureTask<>(task);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        FutureTask<T> futureTask = new FutureTask<>(task, result);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public Future<?> submit(Runnable task) {
        FutureTask<Void> futureTask = new FutureTask<>(task, null);
        execute(futureTask);
        return futureTask;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        List<Future<T>> futures = new ArrayList<>();
        for (Callable<T> task : tasks) {
            futures.add(submit(task));
        }
        for (Future<T> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                // Ignore execution exceptions
            }
        }
        return futures;
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) 
            throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        List<Future<T>> futures = new ArrayList<>();
        try {
            for (Callable<T> task : tasks) {
                futures.add(submit(task));
            }
            for (Future<T> future : futures) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new InterruptedException("Timeout elapsed");
                }
                try {
                    future.get(remaining, TimeUnit.NANOSECONDS);
                } catch (ExecutionException | TimeoutException e) {
                    // Ignore execution and timeout exceptions
                }
            }
            return futures;
        } catch (InterruptedException e) {
            for (Future<T> future : futures) {
                future.cancel(true);
            }
            throw e;
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        try {
            return doInvokeAny(tasks, false, 0);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unexpected TimeoutException");
        }
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return doInvokeAny(tasks, true, unit.toNanos(timeout));
    }

    private <T> T doInvokeAny(Collection<? extends Callable<T>> tasks, boolean timed, long nanos)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<Future<T>> futures = new ArrayList<>();
        try {
            ExecutionException ee = null;
            long deadline = timed ? System.nanoTime() + nanos : 0;
            for (Callable<T> task : tasks) {
                futures.add(submit(task));
            }
            int remaining = futures.size();
            while (remaining > 0) {
                for (Future<T> future : futures) {
                    if (future.isDone()) {
                        try {
                            return future.get();
                        } catch (ExecutionException e) {
                            ee = e;
                        }
                    }
                }
                if (timed && System.nanoTime() > deadline) {
                    throw new TimeoutException();
                }
                Thread.yield();
            }
            throw ee != null ? ee : new ExecutionException("No task completed successfully", null);
        } finally {
            for (Future<T> future : futures) {
                future.cancel(true);
            }
        }
    }
    
    @Override
    public void shutdown() {
        mainLock.lock();
        try {
            isShutdown = true;
            for (Worker worker : workers) {
                worker.interruptIfIdle();
            }
        } finally {
            mainLock.unlock();
        }
    }
    
    @Override
    public List<Runnable> shutdownNow() {
        mainLock.lock();
        try {
            isShutdown = true;
            isTerminated = true;
            List<Runnable> remainingTasks = new ArrayList<>();
            for (Worker worker : workers) {
                worker.thread.interrupt();
                remainingTasks.addAll(worker.queue);
            }
            return remainingTasks;
        } finally {
            mainLock.unlock();
        }
    }

    @Override
    public boolean isShutdown() {
        return isShutdown;
    }

    @Override
    public boolean isTerminated() {
        return isTerminated;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (!isTerminated) {
            if (System.nanoTime() > deadline) {
                return false;
            }
            Thread.sleep(100);
        }
        return true;
    }
    
    private class Worker implements Runnable {
        private final BlockingQueue<Runnable> queue;
        private final Thread thread;
        private volatile boolean running = true;
        
        Worker(BlockingQueue<Runnable> queue) {
            this.queue = queue;
            this.thread = threadFactory.newThread(this);
        }
        
        @Override
        public void run() {
            try {
                while (running && !isShutdown) {
                    Runnable task = queue.poll(keepAliveTime, timeUnit);
                    if (task != null) {
                        activeThreads.incrementAndGet();
                        try {
                            logger.info("[Worker] {} executes {}", thread.getName(), task);
                            task.run();
                        } finally {
                            activeThreads.decrementAndGet();
                        }
                    } else if (activeThreads.get() > corePoolSize) {
                        // Idle timeout for non-core threads
                        logger.info("[Worker] {} idle timeout, stopping.", thread.getName());
                        running = false;
                    }
                }
            } catch (InterruptedException e) {
                running = false;
            } finally {
                logger.info("[Worker] {} terminated.", thread.getName());
            }
        }
        
        void interruptIfIdle() {
            thread.interrupt();
        }
    }
} 