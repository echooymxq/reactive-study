package mxq.echooy.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.ServerTransport;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.transport.netty.server.WebsocketServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class TransportDemo {

    enum TransportType {
        WEBSOCKET, TCP
    }

    @Test
    public void websocket() {
        Mono<String> result = setupServerAndRequest(TransportType.WEBSOCKET, 9210);
        StepVerifier.create(result)
                .expectNext("hello mxq")
                .verifyComplete();

        result = setupServerAndRequest(TransportType.TCP, 9211);
        StepVerifier.create(result)
                .expectNext("hello mxq")
                .verifyComplete();
    }

    private static Mono<String> setupServerAndRequest(TransportType transportType, int port) {
        ServerTransport<?> serverTransport;
        ClientTransport clientTransport;
        if (transportType == TransportType.WEBSOCKET) {
            serverTransport = WebsocketServerTransport.create(port);
            clientTransport = WebsocketClientTransport.create(port);
        } else {
            serverTransport = TcpServerTransport.create(port);
            clientTransport = TcpClientTransport.create(port);
        }
        RSocketServer.create()
                .acceptor((setup, sendingSocket) -> Mono.just(new RSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        String data = payload.getDataUtf8();
                        payload.release();
                        return Mono.just(DefaultPayload.create("hello " + data));
                    }
                }))
                .bindNow(serverTransport);

        return RSocketConnector.create()
                .connect(clientTransport)
                .flatMap(rSocket -> {
                    Payload requestPayload = DefaultPayload.create("mxq");
                    return rSocket.requestResponse(requestPayload);
                })
                .map(payload -> {
                    String data = payload.getDataUtf8();
                    payload.release();
                    return data;
                });
    }

}
