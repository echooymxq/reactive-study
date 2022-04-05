package mxq.echooy.rsocket;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import org.junit.Test;
import reactor.core.publisher.Mono;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class RSocketDemo {

    @Test
    public void payload() {
        TcpServerTransport transport = TcpServerTransport.create(9200);
        RSocketServer.create()
                .acceptor((setup, sendingSocket) -> {
                    String version = setup.getDataUtf8();
                    System.out.println("version:" + version);
                    return Mono.just(new RSocket() {
                        @Override
                        public Mono<Payload> requestResponse(Payload payload) {
                            String data = payload.getDataUtf8();
                            payload.release();
                            return Mono.just(DefaultPayload.create("hello " + data));
                        }
                    });
                })
                .bindNow(transport);

        Payload setupPayload = DefaultPayload.create("v1.0");

        RSocketConnector.create()
                .setupPayload(setupPayload)
                .connect(TcpClientTransport.create(9200))
                .flatMap(rSocket -> {
                    Payload requestPayload = DefaultPayload.create("mxq");
                    return rSocket.requestResponse(requestPayload);
                })
                .map(payload -> {
                    String result = payload.getDataUtf8();
                    payload.release();
                    return result;
                })
                .doOnNext(s -> System.out.println("receive:" + s))
                .block();
    }

}
