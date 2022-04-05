package mxq.echooy.reactor.core.issue;

import org.junit.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.Stream;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class Cache_Replay_Publish_Diff {

    @Test
    // cache相当于执行replay().autoConnect(1)，当第一个订阅者订阅的时候，就会执行connect。
    public void cache() throws InterruptedException {
        Flux<Integer> flux = Flux.fromStream(Stream.of(1, 2, 3, 4, 5))
                .delayElements(Duration.ofMillis(100))
                .cache();
        flux.doOnNext(v -> System.out.println("First: " + v)).subscribe();
        Thread.sleep(1000);

        flux.doOnNext(v -> System.out.println("Second: " + v))
                .subscribe();
        Thread.sleep(100);
    }

    @Test
    // replay则总能看到全部数据。
    public void replay() throws InterruptedException {
        ConnectableFlux<Integer> flux = Flux.fromStream(Stream.of(1, 2, 3, 4, 5))
                .delayElements(Duration.ofSeconds(1))
                .replay();
        flux.doOnNext(v -> System.out.println("First: " + v))
                .subscribe();

        flux.connect();
        Thread.sleep(5000);

        flux.doOnNext(v -> System.out.println("Second: " + v))
                .subscribe();
        Thread.sleep(10000);
    }

    @Test
    // Publish在执行connect之前的订阅者可以看到全部数据，而在connect之后的订阅者只能看到最新的元素。
    public void publish() throws InterruptedException {
        ConnectableFlux<Integer> flux = Flux.fromStream(Stream.of(1, 2, 3, 4, 5))
                .delayElements(Duration.ofSeconds(1))
                .publish();
        flux.doOnNext(v -> System.out.println("First: " + v))
                .subscribe();

        flux.connect();
        Thread.sleep(5000);

        flux.doOnNext(v -> System.out.println("Second: " + v))
                .subscribe();
        Thread.sleep(10000);
    }

}
