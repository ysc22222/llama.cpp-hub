package org.mark.llamacpp.server.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 只扫描 Anthropic 请求 JSON 的顶层 object。
 * 目标是快速提取 model/nodeId/stream 等小字段，并记录大字段的字节范围。
 */
public final class AnthropicTopLevelIndexer {

	public IndexResult index(InputStream inputStream) throws IOException {
		TrackingInput in = new TrackingInput(inputStream);
		int first = in.readNonWhitespace();
		if (first != '{') {
			throw new IOException("Request body must be a JSON object");
		}

		Map<String, JsonMemberRange> members = new LinkedHashMap<>();
		String model = null;
		String nodeId = null;
		Boolean stream = null;

		for (;;) {
			int next = in.readNonWhitespace();
			if (next == '}') {
				break;
			}
			if (next != '"') {
				throw new IOException("Invalid JSON object field");
			}

			long memberStart = in.position() - 1;
			String fieldName = readJsonString(in);
			int colon = in.readNonWhitespace();
			if (colon != ':') {
				throw new IOException("Invalid JSON object field separator");
			}

			int valueFirst = in.readNonWhitespace();
			if (valueFirst == -1) {
				throw new IOException("Unexpected EOF while reading JSON value");
			}
			long valueStart = in.position() - 1;

			Object parsed = parseValue(in, valueFirst, fieldName);
			long valueEnd = in.position();
			members.put(fieldName, new JsonMemberRange(fieldName, memberStart, valueStart, valueEnd, valueEnd));

			if ("model".equals(fieldName) && parsed instanceof String parsedModel) {
				model = parsedModel;
			} else if ("nodeId".equals(fieldName) && parsed instanceof String parsedNodeId) {
				nodeId = parsedNodeId;
			} else if ("stream".equals(fieldName) && parsed instanceof Boolean parsedStream) {
				stream = parsedStream;
			}

			int separator = in.readNonWhitespace();
			if (separator == ',') {
				continue;
			}
			if (separator == '}') {
				break;
			}
			throw new IOException("Invalid JSON object terminator");
		}

		return new IndexResult(model, nodeId, stream, members);
	}

	private static Object parseValue(TrackingInput in, int firstByte, String fieldName) throws IOException {
		if (firstByte == '"') {
			String text = readJsonString(in);
			if ("model".equals(fieldName) || "nodeId".equals(fieldName)) {
				return text;
			}
			return text;
		}
		if (firstByte == '{' || firstByte == '[') {
			skipComposite(in, firstByte);
			return null;
		}
		String literal = readPrimitiveLiteral(in, firstByte);
		if ("stream".equals(fieldName)) {
			if ("true".equals(literal)) {
				return Boolean.TRUE;
			}
			if ("false".equals(literal)) {
				return Boolean.FALSE;
			}
		}
		return literal;
	}

	private static String readJsonString(TrackingInput in) throws IOException {
		StringBuilder sb = new StringBuilder();
		ByteArrayOutputStream raw = new ByteArrayOutputStream();

		for (;;) {
			int ch = in.read();
			if (ch == -1) {
				throw new IOException("Unexpected EOF in JSON string");
			}
			if (ch == '"') {
				if (raw.size() > 0) {
					sb.append(raw.toString(StandardCharsets.UTF_8));
				}
				return sb.toString();
			}
			if (ch == '\\') {
				if (raw.size() > 0) {
					sb.append(raw.toString(StandardCharsets.UTF_8));
					raw.reset();
				}
				int escaped = in.read();
				if (escaped == -1) {
					throw new IOException("Unexpected EOF in JSON escape");
				}
				switch (escaped) {
					case '"':
					case '\\':
					case '/':
						sb.append((char) escaped);
						break;
					case 'b':
						sb.append('\b');
						break;
					case 'f':
						sb.append('\f');
						break;
					case 'n':
						sb.append('\n');
						break;
					case 'r':
						sb.append('\r');
						break;
					case 't':
						sb.append('\t');
						break;
					case 'u':
						sb.append(readUnicodeEscape(in));
						break;
					default:
						throw new IOException("Unsupported JSON escape sequence");
				}
				continue;
			}
			raw.write(ch);
		}
	}

	private static char readUnicodeEscape(TrackingInput in) throws IOException {
		int value = 0;
		for (int i = 0; i < 4; i++) {
			int hex = in.read();
			if (hex == -1) {
				throw new IOException("Unexpected EOF in unicode escape");
			}
			int digit = Character.digit((char) hex, 16);
			if (digit < 0) {
				throw new IOException("Invalid unicode escape");
			}
			value = (value << 4) | digit;
		}
		return (char) value;
	}

	private static void skipComposite(TrackingInput in, int firstByte) throws IOException {
		int depth = 1;
		for (;;) {
			int ch = in.read();
			if (ch == -1) {
				throw new IOException("Unexpected EOF in composite JSON value");
			}
			if (ch == '"') {
				readJsonString(in);
				continue;
			}
			if (ch == '{' || ch == '[') {
				depth++;
				continue;
			}
			if (ch == '}' || ch == ']') {
				depth--;
				if (depth == 0) {
					return;
				}
			}
		}
	}

	private static String readPrimitiveLiteral(TrackingInput in, int firstByte) throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append((char) firstByte);
		for (;;) {
			int ch = in.read();
			if (ch == -1) {
				return sb.toString();
			}
			if (Character.isWhitespace(ch) || ch == ',' || ch == '}' || ch == ']') {
				in.unread(ch);
				return sb.toString();
			}
			sb.append((char) ch);
		}
	}

	public static final class IndexResult {
		private final String model;
		private final String nodeId;
		private final Boolean stream;
		private final Map<String, JsonMemberRange> members;

		private IndexResult(String model, String nodeId, Boolean stream, Map<String, JsonMemberRange> members) {
			this.model = model;
			this.nodeId = nodeId;
			this.stream = stream;
			this.members = Collections.unmodifiableMap(new LinkedHashMap<>(members));
		}

		public String getModel() {
			return model;
		}

		public String getNodeId() {
			return nodeId;
		}

		public Boolean getStream() {
			return stream;
		}

		public Map<String, JsonMemberRange> getMembers() {
			return members;
		}

		public JsonMemberRange getMember(String name) {
			return this.members.get(name);
		}
	}

	public static final class JsonMemberRange {
		private final String name;
		private final long memberStart;
		private final long valueStart;
		private final long valueEnd;
		private final long memberEnd;

		private JsonMemberRange(String name, long memberStart, long valueStart, long valueEnd, long memberEnd) {
			this.name = name;
			this.memberStart = memberStart;
			this.valueStart = valueStart;
			this.valueEnd = valueEnd;
			this.memberEnd = memberEnd;
		}

		public String getName() {
			return name;
		}

		public long getMemberStart() {
			return memberStart;
		}

		public long getValueStart() {
			return valueStart;
		}

		public long getValueEnd() {
			return valueEnd;
		}

		public long getMemberEnd() {
			return memberEnd;
		}
	}

	private static final class TrackingInput {
		private final PushbackInputStream input;
		private long position;

		private TrackingInput(InputStream input) {
			this.input = new PushbackInputStream(input, 8);
			this.position = 0L;
		}

		private int read() throws IOException {
			int ch = this.input.read();
			if (ch != -1) {
				this.position++;
			}
			return ch;
		}

		private void unread(int ch) throws IOException {
			if (ch == -1) {
				return;
			}
			this.input.unread(ch);
			this.position--;
		}

		private int readNonWhitespace() throws IOException {
			for (;;) {
				int ch = this.read();
				if (ch == -1) {
					return -1;
				}
				if (!Character.isWhitespace(ch)) {
					return ch;
				}
			}
		}

		private long position() {
			return this.position;
		}
	}
}
