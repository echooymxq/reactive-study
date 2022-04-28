package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class SchedulerExamples {

    @Test
    public void block_non_blocking_thread() {
        Mono<String> source = Mono.just("a")
                .delayElement(Duration.ofMillis(10)) // delay等时间相关操作符默认使用parallel Scheduler
//                .publishOn(Schedulers.boundedElastic())
                .map(s -> convert(s).block()); // java.lang.IllegalStateException: block()/blockFirst()/blockLast() are blocking, which is not supported in thread parallel

        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
    }

    public static Mono<String> convert(String a) {
        return Mono.defer(() -> Mono.just(a.toUpperCase()));
    }

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
    public void immediate() throws InterruptedException {
        Flux.range(1, 3)
//                .limitRate(2)
                .publishOn(Schedulers.immediate(), 2)  //背压每次读取两个元素，要求不改变线程；此处可以直接替换为limitRate(2)
                .log()
                .subscribe();
        Thread.sleep(500);
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
    public void subscribeOn() {
        Flux<String> source = Flux.just("a")
                .log()
                .map(String::toUpperCase)
                .doOnNext(s -> System.out.println(Thread.currentThread().getName()))
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
    }

    @Test
    // 存在多个subscribeOn时，采用的总是离上游最近的那个
    public void multi_subscribeOn() {
        Flux<String> source = Flux.just("a")
                .log()
                .map(String::toUpperCase)
                .doOnNext(s -> System.out.println(Thread.currentThread().getName()))//Schedulers.parallel()离上游最近，使用Schedulers.parallel调度器线程执行
                .subscribeOn(Schedulers.parallel())
                .subscribeOn(Schedulers.boundedElastic());

        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
    }

    @Test
    public void publishOn() {
        Flux<String> source = Flux.just("a")
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) //main..
                .publishOn(Schedulers.boundedElastic()) // 切换下游上下文执行线程
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) // boundedElastic...
                .log()
                .map(String::toUpperCase);
        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
    }

    @Test
    public void multi_publishOn() {
        Flux<String> source = Flux.just("a")
                .publishOn(Schedulers.parallel()) // 切换下游上下文执行线程
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) //parallel..
                .publishOn(Schedulers.boundedElastic()) // 切换下游上下文执行线程
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) // boundedElastic...
                .log()
                .map(String::toUpperCase);
        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
    }

    @Test
    public void subscribeOn_with_publishOn() {
        Flux<String> source = Flux.just("a")
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) //parallel..
                .publishOn(Schedulers.boundedElastic()) // 切换下游上下文执行线程
                .doOnNext(s -> System.out.println(Thread.currentThread().getName())) // boundedElastic...
                .log()
                .map(String::toUpperCase)
                .subscribeOn(Schedulers.parallel());
        StepVerifier.create(source)
                .expectNext("A")
                .verifyComplete();
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
                .log()
                .map(x -> {
                    String a = String.format("[%s] %s", Thread.currentThread().getName(),
                            x);
                    System.out.println("stage2:" + a);
                    return a;
                })
                .publishOn(Schedulers.boundedElastic())
                .log()
                .map(x -> {
                    String a = String.format("[%s] %s", Thread.currentThread().getName(),
                            x);
                    System.out.println("stage3:" + a);
                    return a;
                })
                .log()
                .subscribeOn(Schedulers.parallel())
                .doFinally(__ -> state.set(true))
                .subscribe();
        await().untilTrue(state);
    }

}
