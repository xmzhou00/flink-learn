package com.producerconsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

public class WaitNotifyModel implements Model {

    private final Object BUFFER_LOCK = new Object();
    private final Queue<Task> buffer = new LinkedList<>();
    private final int cap;
    private final AtomicInteger taskNo = new AtomicInteger(0);

    public WaitNotifyModel(int cap) {
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
            synchronized (BUFFER_LOCK) {
                while (buffer.size() == 0) {
                    BUFFER_LOCK.wait();
                }
                Task task = buffer.poll();
                assert task != null;
                Thread.sleep(500 + (long) (Math.random() * 500));
                log.debug("consume: {}", task.no);
                BUFFER_LOCK.notifyAll();
            }
        }
    }

    private class ProducerImpl extends AbstractProducer {
        private Logger log = LoggerFactory.getLogger(ProducerImpl.class);

        @Override
        public void produce() throws InterruptedException {
            Thread.sleep((long) (Math.random() * 1000));
            synchronized (BUFFER_LOCK) {
                while (buffer.size() == cap) {
                    BUFFER_LOCK.wait();
                }
                Task task = new Task(taskNo.getAndIncrement());
                buffer.offer(task);
                log.debug("producer: {}", task.no);
                BUFFER_LOCK.notifyAll();
            }
        }
    }

    public static void main(String[] args) {
        WaitNotifyModel model = new WaitNotifyModel(3);
        for (int i = 0; i < 2; i++) {
            new Thread(model.newRunnableConsumer()).start();
        }
        for (int i = 0; i < 5; i++) {
            new Thread(model.newRunnableProducer()).start();
        }
    }
}
