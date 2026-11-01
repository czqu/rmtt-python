package net.czqu.rmtt.it;

import io.netty.handler.codec.quic.QuicSslContextBuilder;
import io.netty.handler.ssl.SslContextBuilder;
import net.czqu.rmtt.api.AuthResult;
import net.czqu.rmtt.protocol.ServerKeepalivePolicy;
import net.czqu.rmtt.server.netty.RmttServer;
import net.czqu.rmtt.server.netty.RmttServerBuilder;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * Transport-parameterised RMTT echo server used by the rmtt-python cross-language matrix.
 *
 * <p>Usage: HarnessServerMain &lt;scheme&gt; &lt;port&gt; [cert key]</p>
 *
 * <p>Prints {@code SERVER_READY <scheme> <port>} once bound and echoes every upstream PUSH back
 * as {@code echo:<payload>}.</p>
 */
public final class HarnessServerMain {

    private HarnessServerMain() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("usage: HarnessServerMain <scheme> <port> [cert key]");
            System.exit(2);
        }
        String scheme = args[0];
        int port = Integer.parseInt(args[1]);
        String cert = args.length > 3 ? args[2] : null;
        String key = args.length > 3 ? args[3] : null;
        String reject = "";
        int kpMin = 30;
        int kpMax = 600;
        for (int i = 4; i < args.length; i++) {
            switch (args[i]) {
                case "--reject":
                    reject = args[++i];
                    break;
                case "--kp-min":
                    kpMin = Integer.parseInt(args[++i]);
                    break;
                case "--kp-max":
                    kpMax = Integer.parseInt(args[++i]);
                    break;
                default:
                    break;
            }
        }
        final String rejected = reject;

        final RmttServer[] ref = new RmttServer[1];
        RmttServerBuilder builder = new RmttServerBuilder()
                .authenticator(credential -> rejected.equals(credential)
                        ? AuthResult.reject("rejected by harness") : AuthResult.allow(credential))
                .keepalivePolicy(new ServerKeepalivePolicy(kpMin, kpMax, 60, false))
                .messageHandler((deviceId, payload) -> {
                    System.out.println("PUSH device=" + deviceId + " len=" + payload.length);
                    System.out.flush();
                    byte[] prefix = "echo:".getBytes(StandardCharsets.UTF_8);
                    byte[] echo = new byte[prefix.length + payload.length];
                    System.arraycopy(prefix, 0, echo, 0, prefix.length);
                    System.arraycopy(payload, 0, echo, prefix.length, payload.length);
                    try {
                        ref[0].push(deviceId, echo);
                    } catch (RuntimeException e) {
                        System.err.println("echo push failed: " + e);
                    }
                });

        switch (scheme) {
            case "tcp":
                builder.port(port);
                break;
            case "ws":
                builder.port(0).wsPort(port);
                break;
            case "wss":
                builder.port(0).wssPort(port)
                        .wssSslContext(SslContextBuilder.forServer(new File(cert), new File(key)).build());
                break;
            case "kcp":
                builder.port(0).kcpPort(port);
                break;
            case "quic":
                builder.port(0).quicPort(port)
                        .quicSslContext(QuicSslContextBuilder.forServer(new File(key), null, new File(cert))
                                .applicationProtocols("rmtt").build());
                break;
            default:
                System.err.println("unsupported scheme: " + scheme);
                System.exit(2);
                return;
        }

        RmttServer server = builder.build();
        ref[0] = server;
        server.awaitStartup();
        System.out.println("SERVER_READY " + scheme + " " + port);
        System.out.flush();
        Thread.sleep(Long.MAX_VALUE);
    }
}
