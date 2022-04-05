package mxq.echooy.rsocket.polyglot;

import io.rsocket.Payload;
import io.rsocket.core.RSocketConnector;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;

/**
 * @author <a href="mailto:echooy.mxq@gmail.com">echooymxq</a>
 **/
public class JavaClient {

    public static void main(String[] args) throws InterruptedException {
        RSocketConnector.create()
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
