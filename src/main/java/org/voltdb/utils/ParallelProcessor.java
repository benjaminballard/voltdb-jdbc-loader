package org.voltdb.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class ParallelProcessor<T> {
    static Controller controller;

    public void process(Producer<T> sr, Consumer<T> dr) throws InterruptedException {
        this.controller = new Controller<T>(sr, dr);

        controller.start();

        while (controller.signal()) {
            Thread.sleep(100);
            controller.consumer.join();
        }
    }

    public static class Controller<T> extends Thread {
        private Producer producer;
        private Consumer consumer;
        protected boolean signal = true;  //signal to continue
        protected boolean signalProducer = true;  //signal for producer to continue


        public Controller(Producer producer, Consumer consumer) {
            this.producer = producer;
            this.producer.controller = this;

            this.consumer = consumer;
            this.consumer.controller = this;
        }

        public void run() {
            producer.start();
            consumer.start();
            System.out.println("This will always get printed first");
        }

        protected void signal(boolean b) {
            this.signal = b;
        }

        protected boolean signal() {
            return signal;
        }

        public void signalProducer(boolean b) {
            this.signalProducer = b;
        }

        protected boolean signalProducer() {
            return signalProducer;
        }
    }

    public static class Monitor<T> {
        BlockingQueue<T> jobQueue;

        Monitor(int maxQueueSize) {
            jobQueue = new LinkedBlockingQueue<T>(maxQueueSize);
        }
    }

    public static abstract class Producer<T> extends Thread {
        protected final Monitor<T> monitor;
        public Controller<T> controller;

        public Producer(Monitor<T> monitor) {
            this.monitor = monitor;
            this.setName("Producer");
        }

        public void run() {
            try {
                controller.join();

                while (controller.signal && controller.signalProducer()) {  // end thread when there is nothing more that producer can do
                    producerTask();
                }

            } catch (InterruptedException ie) {
                System.out.println("Producer has been interrupted");
            }
        }

        protected abstract void producerTask();
    }

    public static abstract class Consumer<T> extends Thread {
        protected final Monitor<T> monitor;
        public Controller<T> controller;

        public Consumer(Monitor<T> monitor) {
            this.monitor = monitor;
            this.setName("Consumer");
        }

        public void run() {
            try {
                controller.join();

                while (controller.signal()) {
                    consumerTask();
                }
            } catch (InterruptedException ie) {
                System.out.println("consumer has been interrupted");
            }
        }

        protected abstract void consumerTask();
    }

}