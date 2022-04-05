package mxq.echooy.reactor.core;

import org.awaitility.Duration;
import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class ProcessorExamples {

    static {
        Hooks.onErrorDropped(throwable -> {
            throw new RuntimeException(throwable);
        });
    }

    @Test
    public void sinksOne() {
        Sinks.One<String> sink = Sinks.one();
        sink.tryEmitValue("1");
        StepVerifier.create(sink.asMono())
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    public void sinkEmpty() {
        final Sinks.Empty<Void> sink = Sinks.empty();
        sink.tryEmitEmpty();
        StepVerifier.create(sink.asMono())
                .verifyComplete();
    }

    @Test
    public void sinkMany() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sink.tryEmitNext("1");
        sink.tryEmitNext("2");
        sink.tryEmitNext("3");
        sink.tryEmitComplete();
        StepVerifier.create(sink.asFlux())
                .expectNext("1")
                .expectNext("2")
                .expectNext("3")
                .verifyComplete();
    }

    @Test
    public void sink() {
        final Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        sink.tryEmitNext("1");
        sink.tryEmitNext("2");
        sink.asFlux().subscribe(s -> System.out.println("client1:" + s));
        sink.asFlux().subscribe(s -> System.out.println("client2:" + s));
        sink.tryEmitNext("3");
    }

    @Test(expected = RuntimeException.class)
    // Sink#unicast只允许订阅一次
    public void sink_unicast_only_subscribe_once() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        Flux<String> source = sink.asFlux();
        source.subscribe();
        source.subscribe();
    }

    @Test
    // Sink#multicast多个订阅者
    public void sink_multicast_allow_multi_subscribe() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<String> source = sink.asFlux();
        source.subscribe();
        source.subscribe();
    }

    @Test(expected = Sinks.EmissionException.class)
    public void sink_error_when_subscriber_dispose() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        Flux<String> source = sink.asFlux();
        Disposable disposable = source.subscribe();
        sink.tryEmitNext("1");
        disposable.dispose();
        sink.tryEmitNext("2").orThrow(); // Sink emission failed with FAIL_CANCELLED
    }

    @Test
    // 如果先终止了，后续EmitFailureHandler为true的话，会陷入死循环，导致CPU100%
    public void sink_infinite_loop_when_complete() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sink.tryEmitComplete();
        sink.emitNext("1", (signalType, emitResult) -> {
            System.out.println("signalType:" + signalType + ", emitResult:" + emitResult);
            return true; // FAIL_TERMINATED
        });
    }

    @Test
    public void sink_state() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        assertThat(sink.scan(Scannable.Attr.TERMINATED)).isFalse();
        assertThat(sink.scan(Scannable.Attr.CANCELLED)).isFalse();
        sink.tryEmitComplete();
        assertThat(sink.scan(Scannable.Attr.TERMINATED)).isTrue();
    }

    @Test
    // 多线程并发Sink#emit时线性化问题
    public void multi_thread_sink_emit() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        Scheduler scheduler = Schedulers.newParallel("newParallel");
        for (int tick = 0; tick < 10; tick++) {
            scheduler.schedule(() -> {
                try {
                    sink.tryEmitNext(UUID.randomUUID().toString()).orThrow(); // Sink emission failed with FAIL_NON_SERIALIZED
                } catch (Exception exception) {
                    exception.printStackTrace();
                }
            });
        }
        await().atMost(Duration.ONE_SECOND);
    }

    @Test
    // 默认autoCancel为true的话，就算是multicast,如果是有一个subscriber取消订阅了，那么整个Sinks都会终止.
    public void sink_autoCancel() {
        Sinks.Many<Integer> sink = Sinks.many().multicast()
                .onBackpressureBuffer(Integer.MAX_VALUE, true);
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        sink.asFlux().doOnSubscribe(subscription::set).subscribe(System.out::println);
        subscription.get().cancel(); // 取消订阅
        sink.tryEmitNext(1);
        sink.tryEmitNext(2);
        sink.asFlux().subscribe(System.out::println); //订阅并不会下发元素，因为sink已被取消，可设置autoCancel为false，则不会对sink进行取消
        assertThat(sink.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    public void sink_backpressure_buffer() {
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer(2);
        // bufferSize 会根据传参 自行调整创建的queue的size
        // adjustedBatchSize=8
        // bufferSize：缓存第一个Subscriber订阅之前的元素数量
        sink.tryEmitNext(1);
        sink.tryEmitNext(2);
        sink.tryEmitNext(3);
        sink.tryEmitNext(4);
        sink.tryEmitNext(5);
        sink.tryEmitNext(6);
        sink.tryEmitNext(7);
        sink.tryEmitNext(8);
        // 超过bufferSize的会被删掉
        sink.tryEmitNext(9);

        // 只会消费到1-8
        sink.asFlux().subscribe(System.out::println);
    }

    @Test
    public void sink_fail_overflow() {
        Sinks.Many<Integer> sink = Sinks.many().multicast().onBackpressureBuffer(1);
        sink.tryEmitNext(1);
        // bufferSize=1,queue为OneQueue,多余的元素会被删除
        sink.tryEmitNext(2);
        // 只能消费到1
        Disposable disposable = sink.asFlux().subscribe(System.out::println);
        disposable.dispose();
        // 此时由于元素1已经被消费，所以元素3能被保存，
        sink.tryEmitNext(3);
        // 由于Subscriber已经被dispose,所以Subscriber的数量为0，且元素3未被消费，所以4并不能被保存
        // queue不为空，所以emit结果为FAIL_OVERFLOW
        Sinks.EmitResult emitResult = sink.tryEmitNext(4);
        assertThat(emitResult.isFailure()).isTrue();
    }

}
