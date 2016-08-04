# JSON Output Format

This library provides a small interface for outputting JSON reports from your MapReduce jobs alongside Hadoop. I built this because I got tired of constantly rolling my own :) It's perfect for generating small JSON reports about your data, rather than large blocks of text output.

### Setup

`json-output-format` available on Maven central, via Sonatype OSS:

```
<dependency>
    <groupId>com.zackehh</groupId>
    <artifactId>json-output-format</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Usage

It's super simple to use;

1. Extend `JsonOutputFormat`
2. Override `convertKey` and `convertValue`
3. Optionally, override `merge`

### Example

Here is an example of counting into a JSON report (this is a variant of the famous word count example):

```java
/**
 * Counts the number of word occurrences into a JSON format.
 */
public class IntegerJsonOutputFormat extends JsonOutputFormat<Text, IntWritable> {

    /**
     * Converts a Text key to a String key.
     *
     * @param key the Text key.
     * @return a String representation.
     */
    @Override
    protected String convertKey(Text key) {
        return key.toString();
    }

    /**
     * Converts an IntWritable to a JsonNode containing an Integer value.
     *
     * @param value the IntWritable value.
     * @return a JsonNode containing an Integer value.
     */
    @Override
    protected JsonNode convertValue(IntWritable value) {
        return JsonNodeFactory.instance.numberNode(value.get());
    }

    /**
     * Defines a merge strategy for merging clashing keys. In this case
     * we sum the left and right as we're interested in the total.
     *
     * @param left the existing JsonNode value.
     * @param right the new JsonNode value.
     * @return the JsonNode value to persist going forward.
     */
    @Override
    protected JsonNode merge(JsonNode left, JsonNode right) {
        return JsonNodeFactory.instance.numberNode(left.asLong(0) + right.asLong(0));
    }

}
```

In the typical example of a word count, you'd receive output like this:

```
word_one  15
word_two  30
word_three  45
```

Using the above `IntegerJsonOutputFormat`, you'd receive this instead:

```javascript
{
  "word_one": 15,
  "word_two": 30,
  "word_three": 45
}
```

Voila, nice to read output :)

### Customization

By default, your files are written as `json_output-r-<id>.json`, in the traditional Hadoop format. You can customise the initial file name and extension by using the following configuration options:

```java
conf.set("jof.ext", ".bak")         // defaults to ".json"
conf.set("jof.file", "my_filename") // defaults to "json_output"
```