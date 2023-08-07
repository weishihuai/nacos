package com.example.discoveryconsumer;

import rx.Observable;
import rx.Observer;
import rx.functions.Action0;
import rx.functions.Func0;

import java.util.concurrent.ExecutionException;

public class RxJavaDemo {

    // ReactiveX Java  响应式编程框架(android）
    // 观察者模式
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Action0 onCompleted = new Action0() {
            @Override
            public void call() {
                System.out.println("被观察者完成事件: onComplated");
            }
        };

        // 被观察者
        // 创建一个延迟加载的Observable
        Observable<String> observable = Observable.defer(new Func0<Observable<String>>() {
            @Override
            public Observable<String> call() {
                // 创建了一个数据流（Observable）, 包含单个元素的数据流（"登录"字符串）
                Observable<String> observable1 = Observable.from(new String[]{"hello world"});
                // 为该数据流添加一个回调onCompleted
                return observable1.doOnCompleted(onCompleted);
            }
        });

        // 观察者

        /**
         * 当被观察者产生数据时，onNext(Object o)方法会被调用，数据会作为参数传递给该方法，观察者可以在此方法中对数据进行处理。
         * 如果被观察者产生了错误，则onError(Throwable throwable)方法会被调用，
         * 如果被观察者完成了数据的发送，则onCompleted()方法会被调用。
         */
        Observer<String> observer = new Observer<String>() {
            // 处理数据流的完成事件
            public void onCompleted() {
                System.out.println("观察者完成事件: onCompleted");
            }

            // 处理数据流的错误事件
            @Override
            public void onError(Throwable throwable) {
                System.out.println("观察者错误事件: onError");
            }

            // 处理数据流的接收数据的事件
            @Override
            public void onNext(String o) {
                System.out.println("观察者接收到数据:" + o);
            }
        };

        // 建立观察者（Observer）与被观察者（Observable）之间的订阅关系
        observable.subscribe(observer);

        // 将观察者observer与被观察者observable关联起来
        // 使用了toBlocking()方法将observable转换为BlockingObservable
        // 通过toFuture().get()方法来阻塞地获取数据流中的结果，即等待被观察者发送数据并处理。
        String result = observable.toBlocking().toFuture().get();
        System.out.println("数据流中的结果：" + result);
    }
}
