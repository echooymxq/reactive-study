package mxq.echooy.reactor.core.issue;

import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.util.Arrays;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class Cold_Hot_Diff {

    @Test
    // 发布者每次订阅都会创建新的数据，如果没有订阅，数据永远不会产生。
    public void cold() {
        Flux<String> source = Flux.fromIterable(Arrays.asList("1", "2", "3"));
        source.subscribe(d -> System.out.println("a: " + d));
        source.subscribe(d -> System.out.println("b: " + d));
    }

    @Test
    //发布者不依赖任何订阅者，它们会立即发布数据，当订阅者订阅时，只能看到最新的数据
    public void hot() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        Flux<String> source = sink.asFlux();

        source.subscribe(d -> System.out.println("a: " + d));

        sink.tryEmitNext("1");
        sink.tryEmitNext("2");

        source.subscribe(d -> System.out.println("b: " + d));

        sink.tryEmitNext("3");
        sink.tryEmitNext("4");
        sink.tryEmitComplete();
    }

}
