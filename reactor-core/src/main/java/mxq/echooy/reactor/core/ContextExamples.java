package mxq.echooy.reactor.core;

import org.junit.Test;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class ContextExamples {

    @Test
    public void contextV1() {
        Flux
                .deferContextual(ctx -> {
                    System.out.println("name:" + ctx.get("name"));
                    return Mono.never();
                })
                .log()
                .contextWrite(Context.of("name", "echooymxq"))
                .subscribe();
    }

    @Test
    // Context只能被上游操作符看到
    public void contextV2() {
        Flux.range(1, 2)
                .log()
                .contextWrite(ctx -> ctx.put("ctx", ctx.get("ctx") + "_range"))
                .flatMapSequential(d ->
                        Mono.deferContextual(ctx -> {
                            System.out.println("ctx:" + ctx.get("ctx"));
                            return Mono.empty();
                        })
                )
                .take(1)
                .contextWrite(ctx -> ctx.put("ctx", ctx.get("ctx") + "_take"))
                .subscribe(new BaseSubscriber<>() {
                    public Context currentContext() {
                        return Context.empty()
                                .put("ctx", "baseSubscriber");
                    }
                });
    }

}
