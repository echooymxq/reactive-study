import {BufferEncoders, RSocketClient,} from 'rsocket-core';
import RSocketTcpClient from 'rsocket-tcp-client';

const client = new RSocketClient({
    setup: {
        keepAlive: 60000,
        lifetime: 180000,
        dataMimeType: null,
        metadataMimeType: null,
    },
    transport: new RSocketTcpClient({host: "127.0.0.1", port: 9200},
        BufferEncoders),
});

client.connect().subscribe({
    onComplete: socket => {
        socket.requestResponse({
            data: Buffer.from('mxq', 'utf8'),
            metadata: null,
        }).subscribe({
            onComplete: (payload) => console.log('Request-response result %s', payload.data)
        });
    },
    onError: error => console.error(error),
    onSubscribe: cancel => {
    }
});
