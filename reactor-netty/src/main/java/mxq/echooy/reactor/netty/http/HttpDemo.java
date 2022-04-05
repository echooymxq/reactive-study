package mxq.echooy.reactor.netty.http;

import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.netty.http.server.HttpServer;

import java.net.InetSocketAddress;
import java.time.Duration;

import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class HttpDemo {

    public static void main(String[] args) {
        startServer();

        String result = HttpClient.create()
                .wiretap(true)
                .responseTimeout(Duration.ofSeconds(20))
                .remoteAddress(() -> new InetSocketAddress("127.0.0.1", 8201))
                .get()
                .uri("/hello")
                .responseContent()
                .aggregate()
                .asString()
                .block();
    }

    private static void startServer() {
        HttpServer server = HttpServer
                .create()
                .port(8201)
                .route(r -> r.get("/hello",
                        (req, res) -> res.header(CONTENT_TYPE, TEXT_PLAIN)
                                .sendString(Mono.just("hello world"))));
        server.bindNow();
    }

}
