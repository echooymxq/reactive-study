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

import java.time.Duration;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class KeepAliveDemo {

    @Test
    public void keepAlive() {
        RSocketServer.create()
                .acceptor((setup, sendingSocket) -> Mono.just(new RSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        // 模拟阻塞IO线程，服务端将不能发送KeepAlive Frame.
                        try {
                            Thread.currentThread().join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        return Mono.empty();
                    }
                })).bindNow(TcpServerTransport.create(9999));


        RSocketConnector.create()
                // 第一个参数: 发送keepAlive Frame的间隔时间, 第二个参数: 最后一次KeepAlive的最大超时时间.
                .keepAlive(Duration.ofSeconds(5), Duration.ofSeconds(10))
                .connect(TcpClientTransport.create(9999))
                .flatMap(rSocket -> rSocket.requestResponse(DefaultPayload.create("")))
                .block(); // No keep-alive acks for 10000 ms
    }

}
