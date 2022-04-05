package mxq.echooy.rsocket.rpc;

import com.google.protobuf.Empty;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import mxq.echooy.rsocket.rpc.impl.SimpleServiceImpl;
import mxq.echooy.rsocket.rpc.proto.SimpleRequest;
import mxq.echooy.rsocket.rpc.proto.SimpleResponse;
import mxq.echooy.rsocket.rpc.proto.SimpleServiceClient;
import mxq.echooy.rsocket.rpc.proto.SimpleServiceServer;
import org.junit.Before;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class RSocketRpcDemo {

    @Before
    public void startRPCServer() {
        SimpleServiceServer serviceServer = new SimpleServiceServer(
                new SimpleServiceImpl(), Optional.empty(), Optional.empty(),
                Optional.empty());

        RSocketServer
                .create((setup, sendingSocket) -> Mono.just(serviceServer))
                .bindNow(TcpServerTransport.create(8201));
    }

    @Test
    public void requestResponse() {
        SimpleResponse response = RSocketConnector.create()
                .connect(TcpClientTransport.create(8201))
                .flatMap(rSocket -> {
                    SimpleServiceClient serviceClient = new SimpleServiceClient(rSocket);
                    SimpleRequest simpleRequest = SimpleRequest.newBuilder()
                            .setRequestMessage("1")
                            .build();
                    return serviceClient.requestResponse(simpleRequest);
                }).block();
    }

    @Test
    public void fireAndForget() {
        Empty result = RSocketConnector.create()
                .connect(TcpClientTransport.create(8201))
                .flatMap(rSocket -> {
                    SimpleServiceClient serviceClient = new SimpleServiceClient(rSocket);
                    SimpleRequest simpleRequest = SimpleRequest.newBuilder()
                            .setRequestMessage("fireAndForget")
                            .build();
                    return serviceClient.fireAndForget(simpleRequest);
                }).block();
    }

    @Test
    public void requestStream() {
        RSocketConnector.create()
                .connect(TcpClientTransport.create(8201))
                .flatMapMany(rSocket -> {
                    SimpleServiceClient serviceClient = new SimpleServiceClient(rSocket);
                    SimpleRequest simpleRequest = SimpleRequest.newBuilder()
                            .setRequestMessage("requestStream")
                            .build();
                    return serviceClient.requestStream(simpleRequest);
                })
                .doOnNext(simpleResponse -> {
                    String responseMessage = simpleResponse.getResponseMessage();
                    System.out.println(responseMessage);
                })
                .blockLast();
    }

    @Test
    public void requestChannel() {
        RSocketConnector.create()
                .connect(TcpClientTransport.create(8201))
                .flatMapMany(rSocket -> {
                    SimpleServiceClient serviceClient = new SimpleServiceClient(rSocket);
                    Flux<SimpleRequest> requests = Flux.interval(Duration.ofMillis(200))
                            .map(tick ->
                                    SimpleRequest.newBuilder()
                                            .setRequestMessage(String.valueOf(tick))
                                            .build()
                            );
                    return serviceClient.requestChannel(requests);
                })
                .doOnNext(simpleResponse -> {
                    String responseMessage = simpleResponse.getResponseMessage();
                    System.out.println(responseMessage);
                })
                .blockLast();
    }

}
