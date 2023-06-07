package com.producerconsumer;

import java.util.concurrent.LinkedBlockingQueue;

public class Test {
    public static void main(String[] args) throws InterruptedException {

        LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>(3);


        queue.put("1");
        queue.put("2");
        queue.put("3");


        Thread.sleep(2000);

        queue.take();
        queue.put("4");
        System.out.println("aaaa");
    }
}
