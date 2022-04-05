package mxq.echooy.rsocket.rpc.impl;

import com.google.protobuf.Empty;
import io.netty.buffer.ByteBuf;
import mxq.echooy.rsocket.rpc.proto.SimpleRequest;
import mxq.echooy.rsocket.rpc.proto.SimpleResponse;
import mxq.echooy.rsocket.rpc.proto.SimpleService;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class SimpleServiceImpl implements SimpleService {

    @Override
    public Mono<SimpleResponse> requestResponse(SimpleRequest simpleRequest, ByteBuf metadata) {
        return Mono.fromCallable(() -> SimpleResponse.newBuilder()
                .setResponseMessage("reply " + simpleRequest.getRequestMessage())
                .build());
    }

    @Override
    public Mono<Empty> fireAndForget(SimpleRequest message, ByteBuf metadata) {
        return Mono.just(Empty.getDefaultInstance());
    }

    @Override
    public Flux<SimpleResponse> requestStream(SimpleRequest simpleRequest, ByteBuf metadata) {
        return Flux.just(simpleRequest)
                .flatMap(req ->
                        Flux.interval(Duration.ofMillis(200))
                                .map(tick ->
                                        SimpleResponse.newBuilder()
                                                .setResponseMessage(req.getRequestMessage() + ":" + tick)
                                                .build()
                                )
                );

    }

    @Override
    public Flux<SimpleResponse> requestChannel(Publisher<SimpleRequest> requests, ByteBuf metadata) {
        return Flux.from(requests).flatMap(e -> requestResponse(e, metadata));
    }

}
