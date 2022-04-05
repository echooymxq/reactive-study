package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class SchedulerExamples {

    @Test
    public void scheduler() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux.range(1, 3)
                .log("stage-3")
                .subscribeOn(Schedulers.boundedElastic()) // 修改(onNext, onError, onComplete)线程
                .log("stage-2") //
                .subscribeOn(Schedulers.parallel()) // 修改subscribe和request
                .log("stage-1") // main线程执行
                .doOnComplete(() -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

    @Test
    // 基于守护线程实现
    public void schedulerParallel() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux.just(1)
                .subscribeOn(Schedulers.parallel())
                .log()
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

    @Test
    /*
     * 在线程池disposed之前，应用程序不会退出。因此使用doFinally清理资源，使其程序能够正常退出。
     * 如果不这样做，即使所有线程都shutdown了，程序还是依然会挂起，因为它不是基于守护线程实现的。
     */
    public void schedulerNewParallel() {
        AtomicBoolean state = new AtomicBoolean(false);
        Scheduler scheduler = Schedulers.newParallel("newParallel");
        Flux.just(1)
                .subscribeOn(scheduler)
                .doFinally(_unused -> scheduler.dispose())
                .log()
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

    @Test
    // 有且仅有一个后台守护线程执行.
    public void schedulerSingle() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux.just(1)
                .subscribeOn(Schedulers.single())
                .log()
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

    @Test
    public void schedulerBoundedElastic() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux.just(1)
                .subscribeOn(Schedulers.boundedElastic())
                .log()
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

    @Test
    public void publishOn() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux.just("1", "2", "3")
                .publishOn(Schedulers.boundedElastic())
                .log()
                .doFinally(__ -> state.set(true))
                .subscribe(System.out::println);
        await().untilTrue(state);
    }

    @Test
    public void subscribeOn_and_publishOn() {
        AtomicBoolean state = new AtomicBoolean(false);
        Flux
                .create(sink -> {
                    System.out.println("stage1:" + Thread.currentThread().getName());
                    sink.next(Thread.currentThread().getName());
                    sink.complete();
                })
                .publishOn(Schedulers.single())
//                .log()
                .map(x -> {
                    String a = String.format("[%s] %s", Thread.currentThread().getName(),
                            x);
                    System.out.println("stage2:" + a);
                    return a;
                })
                .publishOn(Schedulers.boundedElastic())
//                .log()
                .map(x -> {
                    String a = String.format("[%s] %s", Thread.currentThread().getName(),
                            x);
                    System.out.println("stage3:" + a);
                    return a;
                })
//                .log()
                .subscribeOn(Schedulers.parallel())
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

}
