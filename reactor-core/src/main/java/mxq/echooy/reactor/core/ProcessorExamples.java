package mxq.echooy.reactor.core;

import org.junit.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Scannable;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void processor() {
        // Reactor中的Processor通常并不是作为一个订阅者来使用的，它们被当成是暴露为一个`Flux`或`Mono`给下游的方式，同时以编程的方式进行下发元素
        UnicastProcessor<String> processor = UnicastProcessor.create();
        processor.onNext("1");
        processor.onComplete();
        StepVerifier.create(processor)
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    public void processor_concurrency() {
        UnicastProcessor<Integer> processor = UnicastProcessor.create();
        IntStream.range(0, 1000_000).parallel() // 这里并发调用processor#onNext导致实际元素数量差异
                .forEach(processor::onNext);
        processor.onComplete();
        StepVerifier.create(processor)
                .expectNextCount(1000_000)
                .verifyComplete();
    }

    @Test
    public void one() {
        Sinks.One<String> sink = Sinks.one();
        sink.tryEmitValue("1");
        StepVerifier.create(sink.asMono())
                .expectNext("1")
                .verifyComplete();
    }

    @Test
    public void empty() {
        final Sinks.Empty<Void> sink = Sinks.empty();
        sink.tryEmitEmpty();
        StepVerifier.create(sink.asMono().log())
                .verifyComplete();
    }

    @Test
    public void many() {
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
    // unsafe提供非线性实现，减少并发检测负载
    public void unsafe() {
        Sinks.Many<String> sink = Sinks.unsafe().many().unicast().onBackpressureBuffer();
    }

    @Test(expected = RuntimeException.class)
    // Sink#unicast只允许订阅一次
    public void sink_unicast_only_subscribe_once() {
        Sinks.Many<String> sink = Sinks.many().unicast().onBackpressureBuffer();
        sink.asFlux().subscribe();
        sink.asFlux().subscribe(); // UnicastProcessor allows only a single Subscriber
    }

    @Test
    // Sink#multicast多个订阅者
    public void sink_multicast_allow_multi_subscribe() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<String> source = sink.asFlux();
        source.subscribe();
        source.subscribe();
    }

    @Test
    // 根据请求最小的Subscriber来下发元素，下发不成功时将其缓存
    public void sinkBackpressure() {
        final Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        Sinks.EmitResult emitResult = sink.tryEmitNext("1");
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        sink.asFlux().subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription s) {
                subscriptionRef.set(s);
                s.request(1); //只请求一个元素
            }
            @Override
            protected void hookOnNext(String value) {
                list1.add(value);
            }
        });
        sink.asFlux().subscribe(list2::add);
        emitResult = sink.tryEmitNext("2"); // 第一个Subscriber只能消费1个元素，后续的元素下发将其缓存，并返回下发成功
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        emitResult = sink.tryEmitNext("3");
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        assertThat(list1).isEqualTo(List.of("1")); // 元素缓存，下游并未获取到数据
        assertThat(list2).isEqualTo(List.of());
        subscriptionRef.get().request(2); // 当订阅者重新请求时，获取到缓存的数据
        assertThat(list1).isEqualTo(List.of("1", "2", "3"));
        assertThat(list2).isEqualTo(List.of("2", "3"));
    }

    @Test
    // 当没有订阅者时下发会失败，且不会缓存元素，另外只要任何一个订阅者不能处理元素，也会失败
    public void sinkDirectAllOrNothing() {
        final Sinks.Many<String> sink = Sinks.many().multicast().directAllOrNothing();
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        Sinks.EmitResult emitResult = sink.tryEmitNext("1"); // 没有订阅者时失败，直接删除元素
        assertThat(emitResult == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER).isTrue();
        sink.asFlux().subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription s) {
                s.request(1);
            }
            @Override
            protected void hookOnNext(String value) {
                list1.add(value);
            }
        });
        sink.asFlux().subscribe(list2::add);
        emitResult = sink.tryEmitNext("2");
        assertThat(list1).isEqualTo(List.of("2"));
        assertThat(list2).isEqualTo(List.of("2"));
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        emitResult = sink.tryEmitNext("3");// 第一个订阅者request(1)，表示只能接收处理一个元素，后续的再下发元素也会失败
        assertThat(emitResult == Sinks.EmitResult.FAIL_OVERFLOW).isTrue();
        assertThat(list1).isEqualTo(List.of("2"));
        assertThat(list2).isEqualTo(List.of("2"));
    }

    @Test
    // 只要有任何一个订阅者能处理元素，下发就不会失败
    public void sinkDirectBestEffort() {
        final Sinks.Many<String> sink = Sinks.many().multicast().directBestEffort();
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        Sinks.EmitResult emitResult = sink.tryEmitNext("1");
        assertThat(emitResult == Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER).isTrue();
        sink.asFlux().subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription s) {
                s.request(1);
            }
            @Override
            protected void hookOnNext(String value) {
                list1.add(value);
            }
        });
        sink.asFlux().subscribe(list2::add);
        emitResult = sink.tryEmitNext("2");
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        assertThat(list1).isEqualTo(List.of("2"));
        assertThat(list2).isEqualTo(List.of("2"));
        emitResult = sink.tryEmitNext("3"); //只要有一个订阅者能处理元素，下发就不会失败
        assertThat(emitResult == Sinks.EmitResult.OK).isTrue();
        assertThat(list1).isEqualTo(List.of("2"));
        assertThat(list2).isEqualTo(List.of("2", "3"));
    }

    @Test
    // 对于新的Subscriber重播全部数据
    public void reply_all() {
        Sinks.Many<String> sink = Sinks.many().replay().all();
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        sink.tryEmitNext("1");
        sink.tryEmitNext("2");
        sink.asFlux().subscribe(list1::add);
        sink.tryEmitNext("3");
        sink.asFlux().subscribe(list2::add);
        assertThat(list1).isEqualTo(List.of("1", "2", "3"));
        assertThat(list2).isEqualTo(List.of("1", "2", "3"));
    }
    @Test
    // 对于新的Subscriber根据limit大小进行重播
    public void reply_limit() {
        Sinks.Many<String> sink = Sinks.many().replay().limit(2);
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        sink.tryEmitNext("1");
        sink.tryEmitNext("2");
        sink.asFlux().subscribe(list1::add);
        sink.tryEmitNext("3");
        sink.asFlux().subscribe(list2::add);
        assertThat(list1).isEqualTo(List.of("1", "2", "3"));
        assertThat(list2).isEqualTo(List.of("2", "3"));
    }

    @Test
    // 对于新的Subscriber只重播最近的一次数据
    public void reply_latest() {
        Sinks.Many<String> sink = Sinks.many().replay().latest();
        List<String> list1 = new ArrayList<>(), list2 = new ArrayList<>();
        sink.tryEmitNext("1");
        sink.tryEmitNext("2");
        sink.asFlux().subscribe(list1::add);
        sink.tryEmitNext("3");
        sink.asFlux().subscribe(list2::add);
        assertThat(list1).isEqualTo(List.of("2", "3"));
        assertThat(list2).isEqualTo(List.of("3"));
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
        IntStream.range(0, 100)
                .parallel()
                .forEach(i -> sink.tryEmitNext(String.valueOf(i)).orThrow()); // Sink emission failed with FAIL_NON_SERIALIZED
    }

    @Test
    // 默认autoCancel为true的话，如果有一个Subscriber取消订阅，那么整个Sinks都会终止.
    public void sink_autoCancel() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer(Integer.MAX_VALUE, true);
        sink.tryEmitNext("1");
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        sink.asFlux().doOnSubscribe(subscription::set).subscribe(System.out::println);
        subscription.get().cancel(); // 取消订阅
        sink.tryEmitNext("2");
        sink.asFlux().subscribe(System.out::println); //订阅并不会接收到元素，因为sink已被取消，可设置autoCancel为false，则不会对sink进行取消
        assertThat(sink.scan(Scannable.Attr.CANCELLED)).isTrue();
    }

    @Test
    // bufferSize：缓存第一个Subscriber订阅之前的元素数量
    public void sink_backpressure_buffer() {
        // bufferSize 会根据传参 自行调整创建的queue的size
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer(2);
        // adjustedBatchSize=8
        IntStream.range(1, 10).forEach(s -> sink.tryEmitNext(String.valueOf(s))); // 超过bufferSize的会被删掉
        // 只会消费到1-8
        sink.asFlux().subscribe(System.out::println);
    }

    @Test
    public void sink_fail_overflow() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer(1);
        sink.tryEmitNext("1");
        // bufferSize=1,queue为OneQueue,多余的元素会被删除
        sink.tryEmitNext("2");
        // 只能消费到1
        Disposable disposable = sink.asFlux().subscribe(System.out::println);
        disposable.dispose();
        // 此时由于元素1已经被消费，所以元素3能被保存，
        sink.tryEmitNext("3");
        // 由于Subscriber已经被dispose,所以Subscriber的数量为0，且元素3未被消费，所以4并不能被保存
        // queue不为空，所以emit结果为FAIL_OVERFLOW
        Sinks.EmitResult emitResult = sink.tryEmitNext("4");
        assertThat(emitResult.isFailure()).isTrue();
    }

}
