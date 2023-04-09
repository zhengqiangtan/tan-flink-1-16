package com.tzq;


/**
 * @Title
 * @Author zhengqiang.tan
 * @Date 3/18/23 11:03 PM
 */
public class TestArthas {
    public static void main(String[] args) {
//        mockCpuHigh();
        mockThreadDead();
    }

    public static void mockCpuHigh(){
       new Thread(()->{
           while (true){}
       }).start();
    }


    private  static void mockThreadDead(){
        Object o1 = new Object();
        Object o2 = new Object();

        Thread t1 = new Thread(() -> {
            synchronized (o1) {
                System.out.println(Thread.currentThread() + ": get资源 o1");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (o2) {
                    System.out.println(Thread.currentThread() + ": get资源 o2");
                }
            }


        });

        Thread t2 = new Thread(() -> {
            synchronized (o2) {
                System.out.println(Thread.currentThread() + ": get资源 o2");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (o1) {
                    System.out.println(Thread.currentThread() + ": get资源 o1");
                }
            }


        });

        t1.start();
        t2.start();

    }

}
