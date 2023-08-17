package com.ceilzcx.siestamq.remoting.netty;

import com.ceilzcx.siestamq.common.Pair;
import com.ceilzcx.siestamq.remoting.InvokeCallback;
import com.ceilzcx.siestamq.remoting.RPCHook;
import com.ceilzcx.siestamq.remoting.common.RemotingHelper;
import com.ceilzcx.siestamq.remoting.common.SemaphoreReleaseOnlyOnce;
import com.ceilzcx.siestamq.remoting.exception.RemotingSendRequestException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTimeoutException;
import com.ceilzcx.siestamq.remoting.exception.RemotingTooMuchRequestException;
import com.ceilzcx.siestamq.remoting.protocol.RemotingCommand;
import com.ceilzcx.siestamq.remoting.protocol.RemotingSysResponseCode;
import com.sun.istack.internal.Nullable;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.util.concurrent.Future;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * @author ceilzcx
 * @since 8/12/2022
 */
public abstract class NettyRemotingAbstract {

    /**
     * oneway方式的信号量
     */
    protected final Semaphore semaphoreOneway;

    /**
     * 异步方式的信号量
     */
    protected final Semaphore semaphoreAsync;

    /**
     * key: request id
     * value: 异步返回的future
     */
    protected final ConcurrentMap<Integer, ResponseFuture> responseTable = new ConcurrentHashMap<>(256);

    /**
     * key: request code
     * value: processor & executorService
     */
    protected final HashMap<Integer, Pair<NettyRequestProcessor, ExecutorService>> processorTable = new HashMap<>(64);

    /**
     * 默认的processor, 一般在table中未找到时使用
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultProcessor;

    protected NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    protected volatile SslContext sslContext;

    protected List<RPCHook> rpcHookList = new ArrayList<>();

    protected NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    public void registerRpcHook(RPCHook rpcHook) {
        if (rpcHook != null && !this.rpcHookList.contains(rpcHook)) {
            this.rpcHookList.add(rpcHook);
        }
    }

    public void clearRpcHooks() {
        this.rpcHookList.clear();
    }

    public void doBeforeRpcHooks(String addr, RemotingCommand request) {
        for (RPCHook rpcHook : this.rpcHookList) {
            rpcHook.doBeforeRequest(addr, request);
        }
    }

    public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        for (RPCHook rpcHook : this.rpcHookList) {
            rpcHook.doAfterResponse(addr, request, response);
        }
    }

    /**
     * 重要方法
     * ChannelInboundHandler调用, 用于处理netty中传递的消息, 主要是com.ceilzcx.siestamq.remoting.protocol.RemotingCommand
     * 包括 Request(Server) 和 Response(Client)
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                case REQUEST:
                    this.processRequestCommand(ctx, msg);
                    break;
                case RESPONSE:
                    this.processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    public void processRequestCommand(ChannelHandlerContext ctx, RemotingCommand request) {
        final int opaque = request.getOpaque();
        // 查询选用哪个Processor, 和通过哪个ExecutorService处理线程
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(request.getCode());
        final Pair<NettyRequestProcessor, ExecutorService> pair = matched == null ? this.defaultProcessor : matched;
        if (pair == null) {
            String error = " request type " + request.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), request, response);
            return;
        }

        Runnable runnable = this.buildProcessRequestHandler(ctx, request, pair);

        // 判断是否拒绝该request
        if (pair.getLeft().rejectRequest()) {
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECT REQUEST]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), request, response);
            return;
        }

        try {
            final RequestTask requestTask = new RequestTask(runnable, ctx.channel(), request);
            pair.getRight().submit(requestTask);
        } catch (RejectedExecutionException e) {
            // todo 为什么要这么判断打印日志?
            if (System.currentTimeMillis() % 10000 == 0) {
                // log
            }

            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[OVERLOAD]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), request, response);
        }
    }

    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand response) {
        final int opaque = response.getOpaque();
        final ResponseFuture responseFuture = this.responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponse(response);

            responseTable.remove(opaque);

            if (responseFuture.getInvokeCallback() != null) {
                this.executeInvokeCallback(responseFuture);
            } else {
                responseFuture.putResponse(response);
                responseFuture.release();
            }
        } else {
            // log
        }
    }

    /**
     * 为了多线程处理Request, 放入ExecutorService中
     */
    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand request,
                                                Pair<NettyRequestProcessor, ExecutorService> pair) {
        return () -> {
            RemotingCommand response;

            try {
                String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());

                // todo try-catch
                this.doBeforeRpcHooks(remoteAddr, request);

                response = pair.getLeft().processRequest(ctx, request);

                // todo try-catch
                this.doAfterRpcHooks(remoteAddr, request, response);

                writeResponse(ctx.channel(), request, response);
            } catch (Exception e) {
                if (!request.isOnewayRPC()) {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, e.getMessage());
                    response.setOpaque(request.getOpaque());
                    writeResponse(ctx.channel(), request, response);
                }
            }
        };
    }

    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response) {
        writeResponse(channel, request, response, null);
    }

    /**
     * 将response写入channel中
     */
    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response,
                                     Consumer<Future<?>> consumer) {

        // RemotingMetricsManager
        // 用于保存各种指标, 主要用于指标监控使用
        // https://blog.lv5.moe/p/rocketmq-observability-metrics
        if (request.isOnewayRPC()) {
            return;
        }
        response.setOpaque(request.getOpaque());
        response.markResponseType();

        try {
            channel.writeAndFlush(response).addListener(future -> {
                // log
                // metrics

                if (consumer != null) {
                    consumer.accept(future);
                }
            });
        } catch (Throwable e) {
            // log
            // metrics
        }
    }

    /**
     * client接收到response后的回调函数
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.execute(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Exception e) {
                        // log
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
            }
        } else {
            runInThisThread = true;
        }

        // 线程池使用失败, 直接在当前线程执行
        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Exception e) {
                // log
            } finally {
                responseFuture.release();
            }
        }
    }

    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        int opaque = request.getOpaque();
        final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
        final SocketAddress addr = channel.remoteAddress();

        try {
            this.responseTable.put(opaque, responseFuture);
            channel.writeAndFlush(request).addListener(future -> {
                if (future.isSuccess()) {
                    responseFuture.setSendRequestOK(true);
                    return;
                }
                // 处理netty中报错的情况
                responseFuture.setSendRequestOK(false);
                responseTable.remove(opaque);
                responseFuture.setCause(future.cause());
                responseFuture.putResponse(null);
            });

            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }
            return responseCommand;
        } finally {
            // 走到这代表这个invoke已经结束, 需要删除对应的future
            // 不删除应该不影响流程, 但可以减少内存的开销
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步invoke
     * response通过invokeCallback处理, 参考executeInvokeCallback
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis, final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        final long beginTime = System.currentTimeMillis();
        final int opaque = request.getOpaque();
        // 查询是否还能继续执行async invoke
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginTime;
            // tryAcquire超时了
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            this.responseTable.put(opaque, responseFuture);
            try {
                channel.writeAndFlush(request).addListener(future -> {
                   if (future.isSuccess()) {
                       responseFuture.setSendRequestOK(true);
                       return;
                   }
                   requestFail(opaque);
                });
            } catch (Exception e) {
                // todo 这里不需要remove吗?
                // this.responseTable.remove(opaque);
                responseFuture.release();
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis,
                                this.semaphoreAsync.getQueueLength(),
                                this.semaphoreAsync.availablePermits()
                        );
                throw new RemotingTimeoutException(info);
            }
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {

    }

    // 处理异步请求发送失败
    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
//                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    public abstract ExecutorService getCallbackExecutor();

    /**
     * 处理netty event, 使用ChannelEventListener监听处理
     */
    class NettyEventExecutor {

    }
}
