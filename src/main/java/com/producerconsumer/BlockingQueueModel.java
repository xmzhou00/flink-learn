package com.producerconsumer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 方案1： 通过BlockingQueue。把并发和容量控制都交给BlockingQueue
 */
public class BlockingQueueModel implements Model {

    private final BlockingQueue<Task> queue;
    private final AtomicInteger taskNo = new AtomicInteger(0);

    public BlockingQueueModel(int cap) {
        this.queue = new LinkedBlockingQueue<>(cap);
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
            Task task = queue.take();
            Thread.sleep(500 + (long) (Math.random() * 500));
            log.debug("consume: {}", task.no);
//            System.out.println("consume: " + task.no);
        }
    }

    private class ProducerImpl extends AbstractProducer {
        private Logger log = LoggerFactory.getLogger(ProducerImpl.class);
        @Override
        public void produce() throws InterruptedException {
            Thread.sleep((long) (Math.random() * 1000));
            Task task = new Task(taskNo.getAndIncrement());
            log.debug("producer: {}", task.no);
//            System.out.println("producer: " + task.no);
            queue.put(task);
        }
    }


    public static void main(String[] args) {
        BlockingQueueModel model = new BlockingQueueModel(3);
        for (int i = 0; i < 2; i++) {
            new Thread(model.newRunnableConsumer()).start();
        }

        for (int i = 0; i < 5; i++) {
            new Thread(model.newRunnableProducer()).start();
        }
    }
}
