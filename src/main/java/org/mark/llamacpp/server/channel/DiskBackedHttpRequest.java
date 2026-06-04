package org.mark.llamacpp.server.channel;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * 提供对落盘请求体的直接访问能力，避免后续逻辑被迫先把整个 body 读回内存。
 */
public interface DiskBackedHttpRequest {

	boolean isBodyOnDisk();

	long bodyLength();

	Path bodyPath();

	InputStream openBodyStream() throws IOException;

	String readBodyAsString(Charset charset) throws IOException;
}
