import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class HiveAnalytics {
    private static final Pattern LOG_PATTERN = Pattern.compile(
        "^(\\S+)\\s+-\\s+-\\s+\\[([^\\]]+)\\]\\s+\"([^\"]*)\"\\s+(\\d{3}|-)\\s+(\\S+)"
    );
    private static final Pattern REQUEST_PATTERN = Pattern.compile(
        "^([A-Z]+)\\s+(\\S+)(?:\\s+(HTTP/\\S+))?$"
    );
    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern(
        "dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH
    );
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java HiveAnalytics <log_file> [<log_file> ...]");
            System.exit(1);
        }
        List<Path> files = new ArrayList<>();
        for (String arg : args) {
            files.add(Paths.get(arg));
        }
        try {
            Result result = processFiles(files);
            System.out.println(result.toJson());
        } catch (Exception e) {
            System.err.println("✗ HiveAnalytics failed");
            e.printStackTrace(System.err);
            System.exit(2);
        }
    }
    private static Result processFiles(List<Path> files) throws IOException {
        Map<String, Q1Entry> q1 = new TreeMap<>();
        Map<String, Q2Entry> q2 = new HashMap<>();
        Map<String, Q3Entry> q3 = new TreeMap<>();
        int totalRecords = 0;
        int malformedRecords = 0;
        for (Path filePath : files) {
            if (!Files.exists(filePath)) {
                throw new IOException("File not found: " + filePath);
            }
            try (BufferedReader reader = Files.newBufferedReader(filePath, StandardCharsets.ISO_8859_1)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    ParsedRecord record = parseLine(line);
                    if (record == null) {
                        malformedRecords++;
                        continue;
                    }
                    totalRecords++;
                    String q1Key = record.logDate + "|" + record.statusCode;
                    q1.computeIfAbsent(q1Key, k -> new Q1Entry(record.logDate, record.statusCode))
                      .add(record.bytesTransferred);
                    q2.computeIfAbsent(record.resourcePath, Q2Entry::new)
                      .add(record.bytesTransferred, record.host);
                    String q3Key = record.logDate + "|" + record.logHour;
                    q3.computeIfAbsent(q3Key, k -> new Q3Entry(record.logDate, record.logHour))
                      .add(record.statusCode, record.host);
                }
            }
        }
        List<Map<String, Object>> q1Rows = new ArrayList<>();
        for (Q1Entry entry : q1.values()) {
            q1Rows.add(entry.toMap());
        }
        List<Q2Entry> q2Entries = new ArrayList<>(q2.values());
        q2Entries.sort(Comparator.comparingLong(Q2Entry::getRequestCount).reversed());
        List<Map<String, Object>> q2Rows = new ArrayList<>();
        int rank = 1;
        for (Q2Entry entry : q2Entries) {
            if (rank > 20) break;
            q2Rows.add(entry.toMap(rank));
            rank++;
        }
        List<Map<String, Object>> q3Rows = new ArrayList<>();
        for (Q3Entry entry : q3.values()) {
            q3Rows.add(entry.toMap());
        }
        return new Result(q1Rows, q2Rows, q3Rows, totalRecords, malformedRecords, files.size());
    }
    private static ParsedRecord parseLine(String line) {
        if (line == null || line.trim().isEmpty()) {
            return null;
        }
        Matcher matcher = LOG_PATTERN.matcher(line);
        if (!matcher.find()) {
            return null;
        }
        String host = matcher.group(1);
        String timestamp = matcher.group(2);
        String request = matcher.group(3);
        String status = matcher.group(4);
        String bytesRaw = matcher.group(5);
        if (status.equals("-")) {
            return null;
        }
        int statusCode;
        try {
            statusCode = Integer.parseInt(status);
        } catch (NumberFormatException e) {
            return null;
        }
        long bytesTransferred = 0;
        if (!bytesRaw.equals("-") && !bytesRaw.trim().isEmpty()) {
            try {
                bytesTransferred = Long.parseLong(bytesRaw);
            } catch (NumberFormatException ignored) {
                bytesTransferred = 0;
            }
        }
        String logDate;
        int logHour;
        try {
            ZonedDateTime dt = ZonedDateTime.parse(timestamp, TIMESTAMP_FORMAT);
            logDate = dt.toLocalDate().toString();
            logHour = dt.getHour();
        } catch (Exception e) {
            return null;
        }
        Matcher reqMatcher = REQUEST_PATTERN.matcher(request.trim());
        String httpMethod = "UNKNOWN";
        String resourcePath = request;
        String protocolVersion = "UNKNOWN";
        if (reqMatcher.find()) {
            httpMethod = reqMatcher.group(1);
            resourcePath = reqMatcher.group(2);
            protocolVersion = reqMatcher.group(3) == null ? "UNKNOWN" : reqMatcher.group(3);
        }
        if (resourcePath == null || resourcePath.trim().isEmpty()) {
            resourcePath = "/";
        }
        return new ParsedRecord(host, logDate, logHour, httpMethod, resourcePath,
                protocolVersion, statusCode, bytesTransferred);
    }
    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '"': sb.append("\\\""); break;
                case '\b': sb.append("\\b"); break;
                case '\f': sb.append("\\f"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20 || c > 0x7E) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.toString();
    }
    private static final class ParsedRecord {
        final String host;
        final String logDate;
        final int logHour;
        final String httpMethod;
        final String resourcePath;
        final String protocolVersion;
        final int statusCode;
        final long bytesTransferred;
        ParsedRecord(String host, String logDate, int logHour, String httpMethod,
                     String resourcePath, String protocolVersion,
                     int statusCode, long bytesTransferred) {
            this.host = host;
            this.logDate = logDate;
            this.logHour = logHour;
            this.httpMethod = httpMethod;
            this.resourcePath = resourcePath;
            this.protocolVersion = protocolVersion;
            this.statusCode = statusCode;
            this.bytesTransferred = bytesTransferred;
        }
    }
    private static final class Q1Entry {
        final String logDate;
        final int statusCode;
        long requestCount;
        long totalBytes;
        Q1Entry(String logDate, int statusCode) {
            this.logDate = logDate;
            this.statusCode = statusCode;
            this.requestCount = 0;
            this.totalBytes = 0;
        }
        void add(long bytes) {
            requestCount++;
            totalBytes += bytes;
        }
        Map<String, Object> toMap() {
            Map<String, Object> row = new HashMap<>();
            row.put("log_date", logDate);
            row.put("status_code", statusCode);
            row.put("request_count", requestCount);
            row.put("total_bytes", totalBytes);
            return row;
        }
    }
    private static final class Q2Entry {
        final String resourcePath;
        long requestCount;
        long totalBytes;
        final Set<String> hosts;
        Q2Entry(String resourcePath) {
            this.resourcePath = resourcePath;
            this.requestCount = 0;
            this.totalBytes = 0;
            this.hosts = new HashSet<>();
        }
        void add(long bytes, String host) {
            requestCount++;
            totalBytes += bytes;
            if (host != null) {
                hosts.add(host);
            }
        }
        long getRequestCount() {
            return requestCount;
        }
        Map<String, Object> toMap(int rank) {
            Map<String, Object> row = new HashMap<>();
            row.put("resource_path", resourcePath);
            row.put("request_count", requestCount);
            row.put("total_bytes", totalBytes);
            row.put("distinct_host_count", hosts.size());
            row.put("rank", rank);
            return row;
        }
    }
    private static final class Q3Entry {
        final String logDate;
        final int logHour;
        long totalRequestCount;
        long errorRequestCount;
        final Set<String> errorHosts;
        Q3Entry(String logDate, int logHour) {
            this.logDate = logDate;
            this.logHour = logHour;
            this.totalRequestCount = 0;
            this.errorRequestCount = 0;
            this.errorHosts = new HashSet<>();
        }
        void add(int statusCode, String host) {
            totalRequestCount++;
            if (statusCode >= 400 && statusCode < 600) {
                errorRequestCount++;
                if (host != null) {
                    errorHosts.add(host);
                }
            }
        }
        Map<String, Object> toMap() {
            Map<String, Object> row = new HashMap<>();
            row.put("log_date", logDate);
            row.put("log_hour", logHour);
            row.put("error_request_count", errorRequestCount);
            row.put("total_request_count", totalRequestCount);
            double errorRate = totalRequestCount > 0
                    ? ((double) errorRequestCount) / totalRequestCount
                    : 0.0;
            row.put("error_rate", Double.valueOf(String.format(Locale.ENGLISH, "%.4f", errorRate)));
            row.put("distinct_error_hosts", errorHosts.size());
            return row;
        }
    }
    private static final class Result {
        final List<Map<String, Object>> q1;
        final List<Map<String, Object>> q2;
        final List<Map<String, Object>> q3;
        final int totalRecords;
        final int malformedRecords;
        final int batchCount;
        Result(List<Map<String, Object>> q1, List<Map<String, Object>> q2,
               List<Map<String, Object>> q3, int totalRecords,
               int malformedRecords, int batchCount) {
            this.q1 = q1;
            this.q2 = q2;
            this.q3 = q3;
            this.totalRecords = totalRecords;
            this.malformedRecords = malformedRecords;
            this.batchCount = batchCount;
        }
        String toJson() {
            StringBuilder sb = new StringBuilder();
            sb.append('{');
            appendArray(sb, "q1", q1);
            sb.append(',');
            appendArray(sb, "q2", q2);
            sb.append(',');
            appendArray(sb, "q3", q3);
            sb.append(',');
            sb.append("\"total_records\":").append(totalRecords).append(',');
            sb.append("\"malformed_records\":").append(malformedRecords).append(',');
            sb.append("\"batch_count\":").append(batchCount);
            sb.append('}');
            return sb.toString();
        }
        private void appendArray(StringBuilder sb, String key, List<Map<String, Object>> rows) {
            sb.append('"').append(key).append('"').append(':');
            sb.append('[');
            boolean firstRow = true;
            for (Map<String, Object> row : rows) {
                if (!firstRow) {
                    sb.append(',');
                }
                firstRow = false;
                appendObject(sb, row);
            }
            sb.append(']');
        }
        private void appendObject(StringBuilder sb, Map<String, Object> row) {
            sb.append('{');
            boolean firstEntry = true;
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                if (!firstEntry) {
                    sb.append(',');
                }
                firstEntry = false;
                sb.append('"').append(entry.getKey()).append('"').append(':');
                Object value = entry.getValue();
                if (value instanceof String) {
                    sb.append('"').append(escapeJson((String) value)).append('"');
                } else {
                    sb.append(value.toString());
                }
            }
            sb.append('}');
        }
    }
}
