package org.mark.llamacpp.server.channel;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.mark.llamacpp.server.LlamaServer;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.util.ReferenceCountUtil;

/**
 * 简化版 HTTP 聚合器：
 * 小请求保存在内存，超过阈值后自动写入磁盘，再生成兼容 FullHttpRequest 的请求对象。
 */
public class DiskBackedHttpObjectAggregator extends ChannelInboundHandlerAdapter {

	private final long maxContentLength;
	private final int memoryThreshold;
	private final Path spoolDirectory;

	private PendingRequest pending;

	public DiskBackedHttpObjectAggregator(long maxContentLength, int memoryThreshold) {
		this(maxContentLength, memoryThreshold, LlamaServer.getCachePath().resolve("http-body-cache"));
	}

	public DiskBackedHttpObjectAggregator(long maxContentLength, int memoryThreshold, Path spoolDirectory) {
		if (maxContentLength <= 0) {
			throw new IllegalArgumentException("maxContentLength must be positive");
		}
		if (memoryThreshold <= 0) {
			throw new IllegalArgumentException("memoryThreshold must be positive");
		}
		this.maxContentLength = maxContentLength;
		this.memoryThreshold = memoryThreshold;
		this.spoolDirectory = spoolDirectory;
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof FullHttpRequest) {
			ctx.fireChannelRead(msg);
			return;
		}

		if (msg instanceof HttpRequest request) {
			if (this.pending != null) {
				this.pending.cleanup();
				this.pending = null;
			}
			this.pending = new PendingRequest(request, this.memoryThreshold, this.spoolDirectory);
			DecoderResult decoderResult = request.decoderResult();
			if (decoderResult != null && !decoderResult.isSuccess()) {
				this.pending.decoderResult = decoderResult;
			}
			ReferenceCountUtil.release(msg);
			return;
		}

		if (msg instanceof HttpContent content) {
			try {
				if (this.pending == null) {
					ctx.fireChannelRead(msg);
					return;
				}
				long nextLength = this.pending.bodyLength + content.content().readableBytes();
				if (nextLength > this.maxContentLength) {
					this.pending.cleanup();
					this.pending = null;
					ReferenceCountUtil.release(msg);
					this.sendTooLarge(ctx);
					return;
				}

				this.pending.body.write(content.content());
				this.pending.bodyLength = nextLength;
				DecoderResult decoderResult = content.decoderResult();
				if (decoderResult != null && !decoderResult.isSuccess()) {
					this.pending.decoderResult = decoderResult;
				}

				if (msg instanceof LastHttpContent lastContent) {
					FullHttpRequest aggregated = this.pending.build(lastContent);
					this.pending = null;
					ctx.fireChannelRead(aggregated);
				}
			} finally {
				ReferenceCountUtil.release(msg);
			}
			return;
		}

		ctx.fireChannelRead(msg);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		if (this.pending != null) {
			this.pending.cleanup();
			this.pending = null;
		}
		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		if (this.pending != null) {
			this.pending.cleanup();
			this.pending = null;
		}
		super.exceptionCaught(ctx, cause);
	}

	private void sendTooLarge(ChannelHandlerContext ctx) {
		DefaultFullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE);
		response.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
		ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
	}

	private static final class PendingRequest {
		private final HttpVersion version;
		private final io.netty.handler.codec.http.HttpMethod method;
		private final String uri;
		private final HttpHeaders headers;
		private final SpoolingBody body;
		private long bodyLength;
		private DecoderResult decoderResult;

		private PendingRequest(HttpRequest request, int memoryThreshold, Path spoolDirectory) throws IOException {
			this.version = request.protocolVersion();
			this.method = request.method();
			this.uri = request.uri();
			this.headers = request.headers().copy();
			this.body = new SpoolingBody(memoryThreshold, spoolDirectory);
			this.bodyLength = 0L;
			this.decoderResult = request.decoderResult();
		}

		private FullHttpRequest build(LastHttpContent lastContent) throws IOException {
			HttpHeaders trailingHeaders = lastContent.trailingHeaders().copy();
			byte[] memoryBody = this.body.memoryBytes();
			Path diskBodyPath = this.body.diskPath();
			DiskBackedFullHttpRequest request = new DiskBackedFullHttpRequest(this.version, this.method, this.uri, this.headers,
					trailingHeaders, memoryBody, diskBodyPath, this.bodyLength);
			if (this.decoderResult != null) {
				request.setDecoderResult(this.decoderResult);
			}
			this.body.detach();
			return request;
		}

		private void cleanup() {
			this.body.cleanup();
		}
	}

	private static final class SpoolingBody {
		private final int memoryThreshold;
		private final Path spoolDirectory;
		private final ByteArrayOutputStream memoryBuffer = new ByteArrayOutputStream();
		private OutputStream spoolOutput;
		private Path diskPath;

		private SpoolingBody(int memoryThreshold, Path spoolDirectory) {
			this.memoryThreshold = memoryThreshold;
			this.spoolDirectory = spoolDirectory;
		}

		private void write(ByteBuf buffer) throws IOException {
			int readable = buffer.readableBytes();
			if (readable <= 0) {
				return;
			}
			if (this.spoolOutput != null) {
				buffer.getBytes(buffer.readerIndex(), this.spoolOutput, readable);
				return;
			}
			if (this.memoryBuffer.size() + readable <= this.memoryThreshold) {
				buffer.getBytes(buffer.readerIndex(), this.memoryBuffer, readable);
				return;
			}
			this.spoolToDisk();
			buffer.getBytes(buffer.readerIndex(), this.spoolOutput, readable);
		}

		private byte[] memoryBytes() throws IOException {
			if (this.spoolOutput != null) {
				this.spoolOutput.flush();
			}
			return this.diskPath == null ? this.memoryBuffer.toByteArray() : null;
		}

		private Path diskPath() throws IOException {
			if (this.spoolOutput != null) {
				this.spoolOutput.flush();
			}
			return this.diskPath;
		}

		private void detach() throws IOException {
			if (this.spoolOutput != null) {
				this.spoolOutput.flush();
				this.spoolOutput.close();
				this.spoolOutput = null;
			}
		}

		private void cleanup() {
			try {
				if (this.spoolOutput != null) {
					this.spoolOutput.close();
					this.spoolOutput = null;
				}
			} catch (IOException ignored) {
			}
			if (this.diskPath != null) {
				try {
					Files.deleteIfExists(this.diskPath);
				} catch (IOException ignored) {
				}
				this.diskPath = null;
			}
		}

		private void spoolToDisk() throws IOException {
			Files.createDirectories(this.spoolDirectory);
			this.diskPath = Files.createTempFile(this.spoolDirectory, "http-body-", ".tmp");
			this.spoolOutput = Files.newOutputStream(this.diskPath);
			this.memoryBuffer.writeTo(this.spoolOutput);
			this.memoryBuffer.reset();
		}
	}
}
