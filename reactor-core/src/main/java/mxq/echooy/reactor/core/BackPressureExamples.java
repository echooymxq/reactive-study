package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class BackPressureExamples {

    @Test
    public void backPressure() {
        Flux.range(1, 10)
                .log()
                .subscribe(new BaseSubscriber<>() {
                    protected void hookOnNext(Integer value) {
                        request(1);
                        System.out.println("onNext: " + value);
                    }
                });
    }

    @Test
    public void backPressureCancel() {
        Flux.range(1, 10)
                .log()
                .subscribe(new BaseSubscriber<>() {
                    @Override
                    protected void hookOnNext(Integer value) {
                        request(1);
                        System.out.println("onNext: " + value);
                        if (value == 5) {
                            cancel();
                        }
                    }
                });
    }

}
