package org.mark.llamacpp.server.channel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.IllegalReferenceCountException;

/**
 * 兼容 FullHttpRequest 的磁盘回退请求对象。
 * 常规代码仍可通过 content() 读取，但真正需要处理超大 body 的逻辑
 * 可以通过 DiskBackedHttpRequest 直接访问缓存文件。
 */
public class DiskBackedFullHttpRequest extends DefaultHttpRequest implements FullHttpRequest, DiskBackedHttpRequest {

	private final HttpHeaders trailingHeaders;
	private final SharedBody sharedBody;

	public DiskBackedFullHttpRequest(HttpVersion version, HttpMethod method, String uri, HttpHeaders headers,
			HttpHeaders trailingHeaders, byte[] memoryBody, Path diskBodyPath, long bodyLength) {
		this(version, method, uri, headers, trailingHeaders, new SharedBody(memoryBody, diskBodyPath, bodyLength));
	}

	private DiskBackedFullHttpRequest(HttpVersion version, HttpMethod method, String uri, HttpHeaders headers,
			HttpHeaders trailingHeaders, SharedBody sharedBody) {
		super(version, method, uri, headers);
		this.trailingHeaders = trailingHeaders == null ? new DefaultHttpHeaders() : trailingHeaders;
		this.sharedBody = sharedBody;
	}

	@Override
	public boolean isBodyOnDisk() {
		return this.sharedBody.isBodyOnDisk();
	}

	@Override
	public long bodyLength() {
		return this.sharedBody.bodyLength();
	}

	@Override
	public Path bodyPath() {
		return this.sharedBody.bodyPath();
	}

	@Override
	public InputStream openBodyStream() throws IOException {
		this.ensureAccessible();
		return this.sharedBody.openBodyStream();
	}

	@Override
	public String readBodyAsString(Charset charset) throws IOException {
		this.ensureAccessible();
		return this.sharedBody.readBodyAsString(charset);
	}

	@Override
	public HttpHeaders trailingHeaders() {
		return this.trailingHeaders;
	}

	@Override
	public ByteBuf content() {
		this.ensureAccessible();
		try {
			return this.sharedBody.content();
		} catch (IOException e) {
			throw new IllegalStateException("Failed to read aggregated request body", e);
		}
	}

	@Override
	public int refCnt() {
		return this.sharedBody.refCnt();
	}

	@Override
	public FullHttpRequest retain() {
		this.sharedBody.retain(1);
		return this;
	}

	@Override
	public FullHttpRequest retain(int increment) {
		this.sharedBody.retain(increment);
		return this;
	}

	@Override
	public boolean release() {
		return this.sharedBody.release(1);
	}

	@Override
	public boolean release(int decrement) {
		return this.sharedBody.release(decrement);
	}

	@Override
	public FullHttpRequest touch() {
		return this;
	}

	@Override
	public FullHttpRequest touch(Object hint) {
		return this;
	}

	@Override
	public FullHttpRequest copy() {
		this.ensureAccessible();
		ByteBuf copiedContent = this.content().copy();
		DefaultFullHttpRequest copied = new DefaultFullHttpRequest(protocolVersion(), method(), uri(), copiedContent, headers().copy(),
				trailingHeaders().copy());
		copied.setDecoderResult(decoderResult());
		return copied;
	}

	@Override
	public FullHttpRequest duplicate() {
		return this.retainedDuplicate();
	}

	@Override
	public FullHttpRequest retainedDuplicate() {
		this.ensureAccessible();
		this.sharedBody.retain(1);
		DiskBackedFullHttpRequest duplicated = new DiskBackedFullHttpRequest(protocolVersion(), method(), uri(), headers().copy(),
				trailingHeaders().copy(), this.sharedBody);
		duplicated.setDecoderResult(decoderResult());
		return duplicated;
	}

	@Override
	public FullHttpRequest replace(ByteBuf content) {
		DefaultFullHttpRequest replaced = new DefaultFullHttpRequest(protocolVersion(), method(), uri(), content, headers().copy(),
				trailingHeaders().copy());
		replaced.setDecoderResult(decoderResult());
		return replaced;
	}

	@Override
	public FullHttpRequest setProtocolVersion(HttpVersion version) {
		super.setProtocolVersion(version);
		return this;
	}

	@Override
	public FullHttpRequest setMethod(HttpMethod method) {
		super.setMethod(method);
		return this;
	}

	@Override
	public FullHttpRequest setUri(String uri) {
		super.setUri(uri);
		return this;
	}

	private void ensureAccessible() {
		if (refCnt() <= 0) {
			throw new IllegalReferenceCountException(0);
		}
	}

	private static final class SharedBody {
		private final byte[] memoryBody;
		private final Path diskBodyPath;
		private final long bodyLength;
		private final AtomicInteger refCnt = new AtomicInteger(1);
		private ByteBuf cachedContent;

		private SharedBody(byte[] memoryBody, Path diskBodyPath, long bodyLength) {
			this.memoryBody = memoryBody == null ? new byte[0] : memoryBody;
			this.diskBodyPath = diskBodyPath;
			this.bodyLength = bodyLength;
		}

		private boolean isBodyOnDisk() {
			return this.diskBodyPath != null;
		}

		private long bodyLength() {
			return this.bodyLength;
		}

		private Path bodyPath() {
			return this.diskBodyPath;
		}

		private InputStream openBodyStream() throws IOException {
			if (this.diskBodyPath != null) {
				return Files.newInputStream(this.diskBodyPath);
			}
			return new ByteArrayInputStream(this.memoryBody);
		}

		private String readBodyAsString(Charset charset) throws IOException {
			if (this.diskBodyPath != null) {
				return Files.readString(this.diskBodyPath, charset);
			}
			return new String(this.memoryBody, charset);
		}

		private synchronized ByteBuf content() throws IOException {
			if (this.cachedContent == null) {
				if (this.diskBodyPath != null) {
					this.cachedContent = Unpooled.wrappedBuffer(Files.readAllBytes(this.diskBodyPath));
				} else {
					this.cachedContent = Unpooled.wrappedBuffer(this.memoryBody);
				}
			}
			return this.cachedContent;
		}

		private int refCnt() {
			return this.refCnt.get();
		}

		private void retain(int increment) {
			if (increment <= 0) {
				throw new IllegalArgumentException("increment must be positive");
			}
			for (;;) {
				int current = this.refCnt.get();
				if (current <= 0) {
					throw new IllegalReferenceCountException(current, increment);
				}
				if (this.refCnt.compareAndSet(current, current + increment)) {
					return;
				}
			}
		}

		private boolean release(int decrement) {
			if (decrement <= 0) {
				throw new IllegalArgumentException("decrement must be positive");
			}
			for (;;) {
				int current = this.refCnt.get();
				if (current < decrement) {
					throw new IllegalReferenceCountException(current, -decrement);
				}
				int next = current - decrement;
				if (this.refCnt.compareAndSet(current, next)) {
					if (next == 0) {
						this.deallocate();
						return true;
					}
					return false;
				}
			}
		}

		private synchronized void deallocate() {
			if (this.cachedContent != null) {
				this.cachedContent.release();
				this.cachedContent = null;
			}
			if (this.diskBodyPath != null) {
				try {
					Files.deleteIfExists(this.diskBodyPath);
				} catch (IOException ignored) {
				}
			}
		}
	}
}
