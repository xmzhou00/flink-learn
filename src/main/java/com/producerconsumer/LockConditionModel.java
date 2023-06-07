package com.producerconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockConditionModel implements Model {
    private final Lock BUFFER_LOCK = new ReentrantLock();
    private final Condition BUFFER_CON = BUFFER_LOCK.newCondition();
    private final Queue<Task> buffer = new LinkedList<>();
    private final int cap;
    private AtomicInteger taskNo = new AtomicInteger(0);

    public LockConditionModel(int cap) {
        this.cap = cap;
    }

    @Override
    public Runnable newRunnableConsumer() {
        return new ConsumerImpl();
    }

    @Override
    public Runnable newRunnableProducer() {
        return new ProducerImpl();
    }


    private class ConsumerImpl extends AbstractConsumer {
        private Logger log = LoggerFactory.getLogger(ConsumerImpl.class);

        @Override
        public void consume() throws InterruptedException {
            BUFFER_LOCK.lockInterruptibly();
            try {
                while (buffer.size() == 0) {
                    BUFFER_CON.await();
                }
                Task task = buffer.poll();
                assert task != null;
                Thread.sleep(500 + (long) (Math.random() * 500));
                log.debug("consumer: {}", task.no);
                BUFFER_CON.signalAll();
            } finally {
                BUFFER_LOCK.unlock();
            }
        }
    }

    private class ProducerImpl extends AbstractProducer{
        private Logger log = LoggerFactory.getLogger(ProducerImpl.class);
        @Override
        public void produce() throws InterruptedException {
            Thread.sleep((long) (Math.random() * 1000));
            BUFFER_LOCK.lockInterruptibly();
            try {
                while (buffer.size() == cap) {
                    BUFFER_CON.await();
                }
                Task task = new Task(taskNo.getAndIncrement());
                buffer.offer(task);
                log.debug("producer: {}", task.no);
                BUFFER_CON.signalAll();
            } finally {
                BUFFER_LOCK.unlock();
            }
        }
    }

    public static void main(String[] args) {
        LockConditionModel model = new LockConditionModel(3);
        for (int i = 0; i < 2; i++) {
            new Thread(model.newRunnableConsumer()).start();
        }
        for (int i = 0; i < 5; i++) {
            new Thread(model.newRunnableProducer()).start();
        }
    }
}
