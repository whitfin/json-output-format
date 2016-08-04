package com.zackehh.outputformat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A JSON output format handler for Hadoop MapReduce jobs. Accepts arbitrary types
 * using the callback functions enforced via the abstract class. Both the conversion
 * methods must be provided, and a merging method is optional.
 *
 * @param <K> an arbitrary key type
 * @param <V> an arbitrary value type
 */
abstract public class JsonOutputFormat<K, V> extends FileOutputFormat<K, V> {

    /**
     * An internal JSON mapper to use for JSON conversion.
     */
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Conversion callback for the key field. This callback accepts the input key
     * from the context, and uses the returned String as the field name inside the
     * JSON object.
     *
     * @param key a Writable key type.
     * @return a String key representation
     */
    @SuppressWarnings("WeakerAccess")
    abstract protected String convertKey(K key);

    /**
     * Conversion callback for the value field. This callback accepts the input value
     * from the context, and uses the returned JsonNode as the value inside the JSON
     * object.
     *
     * @param value a Writable value type.
     * @return a JsonNode value representation
     */
    @SuppressWarnings("WeakerAccess")
    abstract protected JsonNode convertValue(V value);

    /**
     * In the case that the field already exists inside the JSON object, you can
     * define a custom merge function. This callback will use the return value as
     * the new value inside the JSON object. Defaults to simply overwriting with
     * the new value.
     *
     * @param left the existing JsonNode value
     * @param right the new JsonNode value
     * @return a JsonNode value to persist
     */
    @SuppressWarnings("WeakerAccess")
    protected JsonNode merge(@SuppressWarnings("UnusedParameters") JsonNode left, JsonNode right) {
        return right;
    }

    /** {@inheritDoc} */
    @Override
    public final RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        Configuration conf = job.getConfiguration();

        String ext  = conf.get("jof.ext", ".json");
        String name = conf.get("jof.file", "json_output");

        FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(job);
        Path file = new Path(committer.getWorkPath(), getUniqueFile(job, name, ext));

        FileSystem fs = file.getFileSystem(conf);
        FSDataOutputStream out = fs.create(file, false);

        return new JsonOutputWriter(out);
    }

    /**
     * Internal class to write out any JSON. We buffer all JSON in memory and only flush to disk on close.
     * This makes writing extremely fast instead of hitting disk every time.
     */
    private class JsonOutputWriter extends RecordWriter<K, V> {

        /**
         * The output stream to write data to.
         */
        private final DataOutputStream out;

        /**
         * Our internal JSON object.
         */
        private final ObjectNode json;

        /**
         * Accepts a stream to write JSON out to and constructs
         * the initial JSON object.
         *
         * @param out the output stream.
         */
        JsonOutputWriter(DataOutputStream out) {
            this.json = mapper.createObjectNode();
            this.out = out;
        }

        /** {@inheritDoc} */
        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            String field = convertKey(key);

            JsonNode left = this.json.path(field);
            JsonNode right = convertValue(value);

            this.json.set(field, left.isMissingNode() ? right : merge(left, right));
        }

        /** {@inheritDoc} */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            this.out.write(mapper.writeValueAsBytes(this.json));
            this.out.close();
        }

    }

}
