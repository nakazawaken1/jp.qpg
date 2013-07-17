import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.Security;
import java.util.concurrent.Executors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.ssl.NotSslRecordException;
import org.jboss.netty.handler.ssl.SslHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.util.CharsetUtil;


public final class Main {

    /**
     * @param args
     */
    public static void main(String[] args) {
        class SSL {
            SSLContext context = null;
            public SSL(String keystore, String password) {
                try {
                    KeyStore ks = KeyStore.getInstance("JKS");
                    InputStream stream = Main.class.getResourceAsStream(keystore);
                    if(stream == null) throw new FileNotFoundException(keystore);
                    ks.load(stream, password.toCharArray());
                    String algorithm = Security.getProperty("ssl.KeyManagerFactory.algorithm");
                    if (algorithm == null) algorithm = "SunX509";
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
                    kmf.init(ks, password.toCharArray());
                    context = SSLContext.getInstance("TLS");
                    context.init(kmf.getKeyManagers(), null, null);
                } catch(Exception e) {
                    new Exception("initialing HTTPS is failed: ", e).printStackTrace();
                }
            }
            public void addLast(ChannelPipeline pipeline) {
                if(context == null) return;
                SSLEngine engine = context.createSSLEngine();
                engine.setUseClientMode(false);
                pipeline.addLast("ssl", new SslHandler(engine));
            }
        }
        final SSL ssl = new SSL(".keystore", "fund_user2013");
        final int maxRequestSize = 1024 * 1024 * 128;
        ChannelFactory factory = new NioServerSocketChannelFactory(Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
        ServerBootstrap bootstrap = new ServerBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            public ChannelPipeline getPipeline() {
                ChannelPipeline pipeline = Channels.pipeline();
                ssl.addLast(pipeline);
                pipeline.addLast("decoder", new HttpRequestDecoder());
                pipeline.addLast("aggregator", new HttpChunkAggregator(maxRequestSize));
                pipeline.addLast("encoder", new HttpResponseEncoder());
                pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
                pipeline.addLast("handler", new SimpleChannelUpstreamHandler() {
                    @Override
                    public void messageReceived(ChannelHandlerContext c, MessageEvent e) throws Exception {
                        HttpRequest request = (HttpRequest) e.getMessage();
                        HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
                        response.setHeader(HttpHeaders.Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
                        String content = "RemoteAddress: " + e.getRemoteAddress() + "\r\n"
                                + "LocalAdress: " + e.getChannel().getLocalAddress() + "\r\n"
                                + "METHOD: " + request.getMethod() + "\r\n"
                                + "VERSION: " + request.getProtocolVersion() + "\r\n"
                                + "REQUEST_URI: " + request.getUri() + "\r\n"
                                + "MIME-TYPE: " + Files.probeContentType(FileSystems.getDefault().getPath(request.getUri())) + "\r\n"
                                + "CONTENT-LENGTH: " + HttpHeaders.getContentLength(request) + "\r\n"
                            ;
                        HttpHeaders.setContentLength(response, content.getBytes(CharsetUtil.UTF_8).length);
                        response.setContent(ChannelBuffers.copiedBuffer(content, CharsetUtil.UTF_8));
                        ChannelFuture future = c.getChannel().write(response);
                        if(!HttpHeaders.isKeepAlive(request)) future.addListener(ChannelFutureListener.CLOSE);
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext c, ExceptionEvent e) throws Exception {
                        Throwable t = e.getCause();
                        if(t instanceof IllegalArgumentException) {
                            //HTTPサーバにHTTPSアクセスがあったとき
                            System.out.println("https access");
                        } else if(t instanceof NotSslRecordException) {
                            //HTTPSサーバにHTTPアクセスがあったとき
                            System.out.println("http access");
                        } else if(t instanceof ClosedChannelException || t instanceof IOException) {
                            //クライアント側でキャンセルされたとき
                            System.out.println("request cancelled");
                        } else {
                            t.printStackTrace();
                        }
                        if(e.getChannel().isConnected()) {
                            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR);
                            HttpHeaders.setContentLength(response, 0);
                            c.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
                        }
                    }
                });
                return pipeline;
            }
        });
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);
        ChannelGroup group = new DefaultChannelGroup("httpServer");
        group.add(bootstrap.bind(new InetSocketAddress(args.length > 0 ? Integer.parseInt(args[0]) : 8877)));
        System.out.println("Enterキーを押すと終了します。");
        try {
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
        group.close().awaitUninterruptibly();
        factory.releaseExternalResources();
    }
}
