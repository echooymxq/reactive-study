package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class MonoExamples {

    @Test
    public void flatmap() {
        Mono<String> source1 = Mono.just("1").flatMap(Mono::just);
        StepVerifier.create(source1).expectNext("1").verifyComplete();

        Flux<String> source2 = Mono.just("1").flatMapMany(Mono::just);// Mono<String> ==> Flux<String>
        StepVerifier.create(source2).expectNext("1").verifyComplete();

        Flux<String> source3 = Mono.just(Flux.just("1"))
                .flatMapMany(Function.identity());// Mono<Flux<String>> ==> Flux<String>
        StepVerifier.create(source3)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    // Mono#fromCallable是`lazy`的，在订阅时调用执行Callable,为每个订阅者单独执行
    // 每次调用可能得到不同的结果，而如果是Mono#just，那么每次调用总是相同的结果。
    public void fromCallable() {
        Mono<String> source = Mono.fromCallable(MonoExamples::hello);
        StepVerifier.create(source)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    //Mono#just是`eager`的，在组装(assembly)阶段时就执行
    public void just() {
        Mono<String> source = Mono.just(hello());
        StepVerifier.create(source)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    public void mono_FormSupplier() {
        Mono<String> source = Mono.fromSupplier(MonoExamples::hello);
        StepVerifier.create(source)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    public void diff() {
        //  打印一次
        Mono.just(hello())
                .repeat(3)
                .subscribe();
        System.out.println("==============");
        // 打印四次
        Mono.fromCallable(() -> Mono.just(hello()))
                .repeat(3)
                .subscribe();
    }

    @Test
    // 取值先Complete的流，并且dispose掉其它未完成的，所以source1会抛出中断异常
    public void firstWithSignal() {
        Mono<Integer> source1 = Mono.fromCallable(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ex) {
                ex.printStackTrace();// throw interrupted exception.
            }
            System.out.println("1");
            return 1;
        }).subscribeOn(Schedulers.parallel());

        Mono<Integer> source2 = Mono.fromCallable(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            System.out.println("2");
            return 2;
        }).subscribeOn(Schedulers.parallel());

        Mono<Integer> source = Mono.firstWithSignal(source1, source2);
        StepVerifier.create(source)
                .expectNext(2)
                .verifyComplete();
    }

    @Test
    // 等待所有的流结束，然后返回Mono<Void>
    public void when() {
        Mono<Integer> source1 = Mono.fromCallable(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ex) {
                ex.printStackTrace();// throw interrupted exception.
            }
            System.out.println("1");
            return 1;
        }).subscribeOn(Schedulers.parallel());

        Mono<Integer> source2 = Mono.fromCallable(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(100);
            } catch (InterruptedException ex) {
                ex.printStackTrace();
            }
            System.out.println("2");
            return 2;
        }).subscribeOn(Schedulers.parallel());

        Mono<Void> source = Mono.when(source1, source2);
        StepVerifier.create(source)
                .verifyComplete();
    }

    @Test
    // 将流转换为Signal事件下发到下游，如onNext,onError,onComplete.
    public void materialize() {
        Mono.just(1)
                .materialize()
                .subscribe(s -> System.out.println(s.getType()));
    }

    @Test
    // 当上游发生OnError事件，进行重试，retry和retryWhen的区别在于，retryWhen是可以附带操作的。
    public void retry() {
        Mono<Integer> source = Mono.just(1)
                .doOnNext(i -> {
                    throw new IllegalArgumentException("BOOM");
                })
//                .retry()
                .retryWhen(Retry.max(3)
                        .doBeforeRetryAsync(rs -> {
                            System.out.println("do before retry");
                            return Mono.empty();
                        }));
        StepVerifier.create(source)
                .expectError()
                .verify();
    }

    private static String hello() {
        System.out.println("Method invoke");
        return "1";
    }

}
