package mxq.echooy.rsocket.polyglot;

import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import io.rsocket.transport.netty.server.TcpServerTransport;
import io.rsocket.util.DefaultPayload;
import reactor.core.publisher.Mono;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class JavaServer {

    public static void main(String[] args) {
        RSocketServer.create()
                .acceptor((setup, sendingSocket) -> Mono.just(new RSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        String message = payload.getDataUtf8();
                        payload.release();
                        return Mono.just(DefaultPayload.create("hello " + message));
                    }
                }))
                .bindNow(TcpServerTransport.create(9200))
                .onClose().block();
    }

}
