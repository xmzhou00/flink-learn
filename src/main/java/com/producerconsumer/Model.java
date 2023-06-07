package com.producerconsumer;

public interface Model {
    Runnable newRunnableConsumer();
    Runnable newRunnableProducer();
}
