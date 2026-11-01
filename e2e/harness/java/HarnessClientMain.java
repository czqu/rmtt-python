package net.czqu.rmtt.it;

import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import net.czqu.rmtt.client.netty.RmttClient;
import net.czqu.rmtt.client.netty.RmttClientBuilder;
import net.czqu.rmtt.protocol.ConnectReturnCode;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Transport-parameterised RMTT echo probe used by the rmtt-python cross-language matrix.
 *
 * <p>Usage: HarnessClientMain &lt;scheme://host:port&gt; &lt;credential&gt;</p>
 *
 * <p>Runs sequential, concurrent, binary and empty-payload round trips, then disconnects. Prints
 * {@code CLIENT_E2E_PASS} on success.</p>
 */
public final class HarnessClientMain {

    private static final int COUNT = 5;

    private HarnessClientMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: HarnessClientMain <url> <credential>");
            System.exit(2);
        }
        URI uri = URI.create(args[0]);
        String scheme = uri.getScheme();
        String host = uri.getHost();
        int port = uri.getPort();
        String credential = args[1];
        // 8092 is the decoder's default frame cap, so stay well below it.
        int binarySize = args.length > 2 ? Integer.parseInt(args[2]) : 4096;

        BlockingQueue<byte[]> inbound = new LinkedBlockingQueue<>();
        RmttClientBuilder builder = new RmttClientBuilder()
                .host(host)
                .port(port)
                .credential(credential)
                .keepAliveSeconds(10)
                .connectTimeoutMillis(10000)
                .autoReconnect(false)
                .connectRetry(false)
                .pushHandler(inbound::add);

        switch (scheme) {
            case "tcp":
                break;
            case "ws":
                builder.webSocket(true);
                break;
            case "wss":
                builder.webSocket(true).secure(true)
                        .sslContext(SslContextBuilder.forClient()
                                .trustManager(InsecureTrustManagerFactory.INSTANCE).build());
                break;
            case "kcp":
                builder.kcp(true);
                break;
            case "quic":
                builder.quic(true).quicSslContext(QuicSslContextBuilder.forClient()
                        .trustManager(InsecureTrustManagerFactory.INSTANCE)
                        .applicationProtocols("rmtt").build());
                break;
            default:
                System.err.println("unsupported scheme: " + scheme);
                System.exit(2);
                return;
        }

        RmttClient client = builder.build();
        if (client.connect() != ConnectReturnCode.CONNECT_ACCEPTED) {
            System.err.println("connect rejected");
            System.exit(1);
        }
        System.out.println("CLIENT_CONNECTED");
        System.out.flush();

        for (int i = 0; i < COUNT; i++) {
            String message = "ping-" + i;
            client.push(message).awaitUninterruptibly(10000);
            if (!waitFor(inbound, ("echo:" + message).getBytes(StandardCharsets.UTF_8), 10)) {
                System.err.println("missing echo for " + message);
                System.exit(1);
            }
        }
        System.out.println("CLIENT_SEQ_OK n=" + COUNT);
        System.out.flush();

        inbound.clear();
        Thread[] threads = new Thread[COUNT];
        for (int i = 0; i < COUNT; i++) {
            final int index = i;
            threads[i] = new Thread(() -> client.push("conc-" + index).awaitUninterruptibly(10000));
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        Set<String> pending = new HashSet<>();
        for (int i = 0; i < COUNT; i++) {
            pending.add("echo:conc-" + i);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (!pending.isEmpty()) {
            byte[] payload = inbound.poll(Math.max(1, TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())),
                    TimeUnit.MILLISECONDS);
            if (payload == null) {
                System.err.println("missing concurrent echoes: " + pending);
                System.exit(1);
            }
            pending.remove(new String(payload, StandardCharsets.UTF_8));
        }
        System.out.println("CLIENT_CONCURRENT_OK n=" + COUNT);
        System.out.flush();

        inbound.clear();
        byte[] blob = new byte[binarySize];
        for (int i = 0; i < blob.length; i++) {
            blob[i] = (byte) i;
        }
        client.push(blob).awaitUninterruptibly(20000);
        byte[] expected = new byte[blob.length + 5];
        System.arraycopy("echo:".getBytes(StandardCharsets.UTF_8), 0, expected, 0, 5);
        System.arraycopy(blob, 0, expected, 5, blob.length);
        if (!waitFor(inbound, expected, 20)) {
            System.err.println("binary echo mismatch");
            System.exit(1);
        }
        System.out.println("CLIENT_BINARY_OK bytes=" + blob.length);
        System.out.flush();

        inbound.clear();
        client.push(new byte[0]).awaitUninterruptibly(10000);
        if (!waitFor(inbound, "echo:".getBytes(StandardCharsets.UTF_8), 10)) {
            System.err.println("empty echo missing");
            System.exit(1);
        }
        System.out.println("CLIENT_EMPTY_OK");
        System.out.flush();

        client.disconnect();
        client.shutdown();
        System.out.println("CLIENT_E2E_PASS");
        // The KCP/QUIC transports leave non-daemon threads behind; exit explicitly.
        System.exit(0);
    }

    private static boolean waitFor(BlockingQueue<byte[]> queue, byte[] want, int seconds)
            throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            byte[] payload = queue.poll(200, TimeUnit.MILLISECONDS);
            if (payload != null && Arrays.equals(payload, want)) {
                return true;
            }
        }
        return false;
    }
}
