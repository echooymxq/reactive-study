package mxq.echooy.reactor.core.issue;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class Concat_Merge_Diff {

    @Test
    // Merge和Concat的区别,Merge并不保证顺序，所以两个流的元素是并行下发到下游的，
    // 而Concat则是会保持流的顺序，需要等待上一个流Complete后才进行下一个流的下发
    public void concat_merge_diff() throws InterruptedException {
        Sinks.Many<String> sink1 = Sinks.many().unicast().onBackpressureBuffer();
        Sinks.Many<String> sink2 = Sinks.many().unicast().onBackpressureBuffer();

        Flux<String> source1 = sink1.asFlux();

        Flux<String> source2 = sink2.asFlux();
        // Flux<String> concat = Flux.concat(source1, source2);
        Flux<String> concat = Flux.merge(source1, source2);
        concat.subscribe(System.out::println);

        System.out.println("Subscribe thread sleep 1s");
        Thread.sleep(10);

        AtomicInteger i = new AtomicInteger();
        Flux.interval(Duration.ofMillis(100))
                .doOnNext(tick -> {
                    Sinks.EmitResult emitResult = sink1.tryEmitNext("sink1   " + i.getAndIncrement());
                    if (emitResult.isFailure()) {
                        System.out.println(emitResult);
                    }
                })
                .subscribe();

        AtomicInteger j = new AtomicInteger();
        Flux.interval(Duration.ofMillis(100))
                .doOnNext(tick -> {
                    Sinks.EmitResult emitResult = sink2.tryEmitNext("sink2   " + j.getAndIncrement());
                    if (emitResult.isFailure()) {
                        System.out.println(emitResult);
                    }
                })
                .subscribe();

        Thread.sleep(2000);
    }

}
