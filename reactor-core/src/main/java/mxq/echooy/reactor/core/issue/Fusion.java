package mxq.echooy.reactor.core.issue;

import org.junit.Test;
import reactor.core.publisher.Flux;

public class Fusion {

    @Test
    public void fusion() {
        Flux.just(1)
                .filter(i -> i != 2)
                .filter(i -> i != 3)
                .log()
                .subscribe(System.out::println);
    }

}
