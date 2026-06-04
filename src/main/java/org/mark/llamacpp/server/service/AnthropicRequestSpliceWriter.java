package org.mark.llamacpp.server.service;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.gson.JsonElement;

import org.mark.llamacpp.server.tools.HttpRequestBodyHandle;
import org.mark.llamacpp.server.tools.JsonUtil;

/**
 * 按顶层成员重组 Anthropic 请求体。
 * 大字段直接从原始缓存文件按偏移复制，小字段由程序生成。
 */
public final class AnthropicRequestSpliceWriter {

	private final HttpRequestBodyHandle bodyHandle;
	private final AnthropicTopLevelIndexer.IndexResult index;

	public AnthropicRequestSpliceWriter(HttpRequestBodyHandle bodyHandle, AnthropicTopLevelIndexer.IndexResult index) {
		this.bodyHandle = bodyHandle;
		this.index = index;
	}

	public void write(OutputStream outputStream, Map<String, JsonElement> overrides, Set<String> removedKeys) throws IOException {
		if (outputStream == null) {
			throw new IllegalArgumentException("outputStream must not be null");
		}
		Map<String, JsonElement> safeOverrides = overrides == null ? Map.of() : new LinkedHashMap<>(overrides);
		Set<String> safeRemoved = removedKeys == null ? Set.of() : removedKeys;

		boolean first = true;
		outputStream.write('{');

		for (AnthropicTopLevelIndexer.JsonMemberRange member : this.index.getMembers().values()) {
			String name = member.getName();
			if (safeRemoved.contains(name) || safeOverrides.containsKey(name)) {
				continue;
			}
			if (!first) {
				outputStream.write(',');
			}
			this.bodyHandle.copyRangeTo(outputStream, member.getMemberStart(), member.getMemberEnd());
			first = false;
		}

		for (Map.Entry<String, JsonElement> entry : safeOverrides.entrySet()) {
			String key = entry.getKey();
			JsonElement value = entry.getValue();
			if (key == null || key.isBlank() || value == null || value.isJsonNull()) {
				continue;
			}
			if (!first) {
				outputStream.write(',');
			}
			outputStream.write(JsonUtil.toJson(key).getBytes(StandardCharsets.UTF_8));
			outputStream.write(':');
			outputStream.write(JsonUtil.toJson(value).getBytes(StandardCharsets.UTF_8));
			first = false;
		}

		outputStream.write('}');
	}
}
