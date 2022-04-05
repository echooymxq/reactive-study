package mxq.echooy.reactor.netty.tcp;

import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelOption;
import reactor.core.publisher.Flux;
import reactor.netty.tcp.TcpClient;
import reactor.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class TcpDemo {

    public static void main(String[] args) {
        startTcpServer();

        TcpClient
                .create()
                .port(8290)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 50000)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .connect()
                .flatMapMany(connection ->
                        Flux.just("start!")
                                .map(s -> Unpooled.wrappedBuffer(s.getBytes(StandardCharsets.UTF_8)))
                                .concatMap(connection.outbound()::sendObject)
                                .thenMany(connection.inbound().receive()))
                .doOnNext(byteBuf -> {
                    byte[] bytes = ByteBufUtil.getBytes(byteBuf);
                    System.out.println(new String(bytes));
                })
                .blockLast();
    }

    private static void startTcpServer() {
        TcpServer.create()
                .port(8290)
                .wiretap(true)
                .doOnConnection(connection -> {
                })
                .handle((in, out) -> in.receive()
                        .asString()
                        .doOnNext(s ->
                                out.sendString(Flux.interval(Duration.ofMillis(100))
                                        .map(String::valueOf))
                                        .then()
                                        .subscribe()
                        )
                        .then())
                .bindNow();
    }

}
