package org.mark.llamacpp.server.tools;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import org.mark.llamacpp.server.channel.DiskBackedHttpRequest;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.util.ReferenceCountUtil;

/**
 * 对请求体做一次稳定快照，避免异步线程继续依赖原始 FullHttpRequest 的生命周期。
 */
public final class HttpRequestBodyHandle implements AutoCloseable {

	private final FullHttpRequest retainedRequest;
	private final DiskBackedHttpRequest diskBackedRequest;
	private final byte[] memoryBytes;
	private final long bodyLength;
	private final Path bodyPath;

	private HttpRequestBodyHandle(FullHttpRequest retainedRequest, DiskBackedHttpRequest diskBackedRequest, byte[] memoryBytes,
			long bodyLength, Path bodyPath) {
		this.retainedRequest = retainedRequest;
		this.diskBackedRequest = diskBackedRequest;
		this.memoryBytes = memoryBytes;
		this.bodyLength = bodyLength;
		this.bodyPath = bodyPath;
	}

	public static HttpRequestBodyHandle capture(FullHttpRequest request) throws IOException {
		if (request instanceof DiskBackedHttpRequest diskBacked && diskBacked.isBodyOnDisk()) {
			FullHttpRequest retained = request.retainedDuplicate();
			DiskBackedHttpRequest retainedDiskBacked = (DiskBackedHttpRequest) retained;
			return new HttpRequestBodyHandle(retained, retainedDiskBacked, null, retainedDiskBacked.bodyLength(), retainedDiskBacked.bodyPath());
		}

		byte[] bytes = new byte[request.content().readableBytes()];
		request.content().getBytes(request.content().readerIndex(), bytes);
		return new HttpRequestBodyHandle(null, null, bytes, bytes.length, null);
	}

	public boolean isBodyOnDisk() {
		return this.diskBackedRequest != null && this.diskBackedRequest.isBodyOnDisk();
	}

	public long bodyLength() {
		return this.bodyLength;
	}

	public Path bodyPath() {
		return this.bodyPath;
	}

	public InputStream openInputStream() throws IOException {
		if (this.diskBackedRequest != null) {
			return this.diskBackedRequest.openBodyStream();
		}
		return new ByteArrayInputStream(this.memoryBytes == null ? new byte[0] : this.memoryBytes);
	}

	public String readUtf8() throws IOException {
		return this.readString(StandardCharsets.UTF_8);
	}

	public String readString(Charset charset) throws IOException {
		if (this.diskBackedRequest != null) {
			return this.diskBackedRequest.readBodyAsString(charset);
		}
		return new String(this.memoryBytes == null ? new byte[0] : this.memoryBytes, charset);
	}

	public String readUtf8Range(long startInclusive, long endExclusive) throws IOException {
		return this.readRange(startInclusive, endExclusive, StandardCharsets.UTF_8);
	}

	public String readRange(long startInclusive, long endExclusive, Charset charset) throws IOException {
		byte[] bytes = this.readRangeBytes(startInclusive, endExclusive);
		return new String(bytes, charset);
	}

	public void copyRangeTo(OutputStream outputStream, long startInclusive, long endExclusive) throws IOException {
		if (outputStream == null) {
			throw new IllegalArgumentException("outputStream must not be null");
		}
		if (endExclusive < startInclusive) {
			throw new IllegalArgumentException("endExclusive must be greater than or equal to startInclusive");
		}
		long length = endExclusive - startInclusive;
		if (length == 0) {
			return;
		}
		if (this.diskBackedRequest != null) {
			try (RandomAccessFile raf = new RandomAccessFile(this.bodyPath.toFile(), "r")) {
				raf.seek(startInclusive);
				byte[] buffer = new byte[8192];
				long remaining = length;
				while (remaining > 0) {
					int read = raf.read(buffer, 0, (int) Math.min(buffer.length, remaining));
					if (read < 0) {
						throw new IOException("Unexpected EOF while copying request body range");
					}
					outputStream.write(buffer, 0, read);
					remaining -= read;
				}
			}
			return;
		}
		outputStream.write(this.readRangeBytes(startInclusive, endExclusive));
	}

	private byte[] readRangeBytes(long startInclusive, long endExclusive) throws IOException {
		if (startInclusive < 0 || endExclusive < startInclusive || endExclusive > this.bodyLength) {
			throw new IOException("Invalid body range: [" + startInclusive + ", " + endExclusive + ")");
		}
		if (this.diskBackedRequest != null) {
			int length = Math.toIntExact(endExclusive - startInclusive);
			byte[] bytes = new byte[length];
			try (RandomAccessFile raf = new RandomAccessFile(this.bodyPath.toFile(), "r")) {
				raf.seek(startInclusive);
				raf.readFully(bytes);
			}
			return bytes;
		}
		return Arrays.copyOfRange(this.memoryBytes == null ? new byte[0] : this.memoryBytes, (int) startInclusive, (int) endExclusive);
	}

	@Override
	public void close() {
		if (this.retainedRequest != null) {
			ReferenceCountUtil.release(this.retainedRequest);
		}
	}
}
