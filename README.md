# data-digger

The data-digger is a simple tool for "digging" through JSON or protobuf-formatted
streams and outputting the approximate
[top K](https://link.springer.com/chapter/10.1007/978-3-642-00887-0_74) values for one or more
message fields. In the process of doing this analysis, it can also output the raw message
bodies (i.e., "tail" style).

Currently, the tool supports reading data in Kafka, S3, or local files. Kafka-sourced messsages can
be in either JSON or protobuf format. S3 and local file sources support
[newline-delimited JSON](https://en.wikipedia.org/wiki/JSON_streaming#Line-delimited_JSON) only.

<img width="1000" alt="digger_screenshot2" src="https://user-images.githubusercontent.com/54862872/96078675-e026ed80-0e67-11eb-9cf3-96b6e0da5556.png">

## Motivation

Many software systems generate and/or consume streams of structured messages; these messages might
represent customer events (as at [Segment](https://segment.com)) or system logs,
for example, and can be stored in local files, Kafka, S3, or other destinations.

It's sometimes useful to scan these streams, apply some filtering, and then either print the
messages out or generate basic summary stats about them. At Segment, for instance, we frequently
do this to debug issues in production, e.g. if a single event type or customer source is
overloading our data pipeline.

The data-digger was developed to support these kinds of use cases in an easy, lightweight way.
It's not as powerful as frameworks like [Presto](https://prestodb.io/), but it's a lot easier
to run and, in conjunction with other tools like [`jq`](https://github.com/stedolan/jq), is often
sufficient for answering basic questions about data streams.

## Installation

Either:

1. Run `GO111MODULE="on" go get github.com/segmentio/data-digger/cmd/digger` *or*
2. Clone this repo and run `make install` in the repo root

The `digger` binary will be placed in `$GOPATH/bin`.

## Quick tour

First, clone this repo and install the `digger` binary as described above.

Then, generate some sample data (requires [Python3](https://www.python.org/downloads/)):

```
./scripts/generate_sample_data.py
```

By default, the script will dump 20 files, each with around 65k JSON-delimited messages, into
the `test_inputs` subdirectory (run with `--help` to see the configuration options).

Each message, in turn, is a generic, Segment-like event that represents a logged
customer interaction:

```
{
  "app: [name of the app where event occurred],
  "context": {
    "os": [user os],
    "version": [user os version]
  },
  "latency": [latency observed by user, in ms],
  "messageId": [id of the message],
  "timestamp": [when the event occurred],
  "type": [interaction type]
}
```

We're now ready to do some digging! Here are some examples to try:

1. Get the top K values for the `app` field:

```
digger file --file-paths=test_inputs --paths='app'
```

2. Get the top K values for the combination of the `app`, `type`, and `os`:

```
digger file --file-paths=test_inputs --paths='app;type;context.os'
```

3. Show the number of events by day:

```
digger file --file-paths=test_inputs --paths='timestamp|@trim:10' --sort-by-name
```

4. Pretty-print all messages that contain the string "oreo" (also requires [`jq`](https://stedolan.github.io/jq/)):

```
digger file --file-paths=test_inputs --filter=oreo --raw | jq
```

5. Get basic stats on the `latency` values by `type`:

```
digger file --file-paths=test_inputs --paths='type;latency' --numeric
```


## Usage

```
digger [source type] [options]
```

Currently, three source types are supported:

1. `kafka`: Read JSON or proto-formatted messages in a Kafka topic.
2. `s3`: Read newline-delimited, JSON formatted messages from the objects in one or more S3
  prefixes.
3. `file`: Read newline-delimited, JSON formatted messages from one or more local file paths.

The common options include:

```json
    --debug               turn on debug logging (default: false)
-f, --filter string       filter regexp to apply before generating stats
-k, --num-categories int  number of top values to show (default: 25)
    --numeric             treat values as numbers instead of strings (default: false)
    --paths string        comma-separated list of paths to generate stats for
    --plugins string      comma-separated list of golang plugins to load at start
    --print-missing       print out messages that missing all paths (default: false)
    --raw                 show raw messages that pass filters (default: false)
    --raw-extended        show extended info about messages that pass filters (default: false)
    --sort-by-name        sort top k values by their category/key names (default: false)
```

Each source also has source-specific options, described in the sections below.

#### Kafka source

The `kafka` subcommand exposes a number of options to configure the underlying Kafka reader:

```
-a, --address string      kafka address
-o, --offset int64        kafka offset (default: -1)
-p, --partitions string   comma-separated list of partitions
    --since duration      time to start at relative to now
-t, --topic string        kafka topic
    --until duration      time to end at relative to now
```

The `address` and `topic` options are required; the others are optional and will default to
reasonable values if omitted (i.e., all partitions starting from the latest message).

#### S3 source

The `s3` source is configured with a bucket, list of prefixes, and (optional) number of workers:

```
-b, --bucket string       s3 bucket
    --num-workers int     number of objects to read in parallel (default: 4)
-p, --prefixes string     comma-separated list of prefixes
```

The objects under each prefix can be compressed provided that the `ContentEncoding` is set
to the appropriate value (e.g., `gzip`).

#### Local file(s) source

The `file` source is configured with a list of paths:

```
  --file-paths string   comma-separated list of file paths
  --resursive           scan directories recursively
```

Each path can be either a file or directory. If `--recursive` is set, then each directory
will be scanned recursively; otherwise, only the top-level files will be processed.

Files with names ending in `.gz` will be assumed to be gzipped compressed. All other files
will be processed as-is.

### Paths syntax

The optional `paths` flag is used to pull out the values that will be used for the top K
stats. All arguments should be in
[gjson syntax](https://github.com/tidwall/gjson/blob/master/SYNTAX.md).

If desired, multiple paths can be combined with either commas or semicolons.
In the comma case, the components will be treated as independent paths and the
*union* of all values will be counted. When using semicolons, the values for each
path or path group will be *intersected* and treated as a single value. If both
commas and semicolons are used, the union takes precedence over the intersection.

If the element at a path is an array, then each item in the array will be treated
as a separate value. The tool doesn't currently support intersections involving array
paths; if a path query would results in more than one intersected value for a single
message, then only the first combination will be counted and the remaining ones will be
dropped.

If `paths` is empty, all messages will be assigned to an `__all__` bucket.

#### Extra gjson modifiers

In addition to the standard `gjson` functionality, the `digger` includes a few
[custom modifiers](https://github.com/tidwall/gjson#modifiers-and-path-chaining) that we've
found helpful for processing data inside Segment:

1. `base64d`: Do a base64 decode on the input
2. `trim`: Trim the input to the argument length

### Outputs

The tool output is determined by the flags it's run with. The most common modes include:

1. No output flags set (default): Only show summary stats while running, then print out a top K
  summary table after an interrupt is detected.
2. `--raw`: Print out raw values of messages after any filtering and/or decoding. Can be piped to
  a downstream tool that expects JSON like `jq`.
3. `--raw-extended`: Like `--raw`, but wraps each message value in a JSON struct that also includes
  message context like the partition (kafka case) or key (s3 case) and offset. Can be piped to
  a downstream tool that expects JSON like `jq`.
4. `--print-missing`: Prints out summary stats plus bodies of any messages that don't match
  the argument paths. Useful for debugging path expressions.
5. `--debug`: Prints out summary stats plus lots of debug messages, including the details of each
  processed message. Intended primarily for tool developers.

### Protocol buffer support

The `kafka` input mode supports processing protobuf types that are in the
[`gogo`](https://github.com/gogo/protobuf) registry in the `digger` binary.

To add protobuf types to the registry either:

1. Clone this repo and import your protobuf types somewhere in the main package *or*
2. Create a golang plugin that includes your protobuf type and run the `digger` with the `--plugins`
  option

Once the types are included, you can use them by running the `kafka` subcommand with the
`--proto-types` option. The values passed to that flag should match the names that your types
are registered as; you can find these names by looking in the `init` function in the generated
go code for your protos.

In the future, we plan on adding support for protobufs registered via the
[v2 API](https://blog.golang.org/protobuf-apiv2). The new API supports iterating over all
registered message types, which should make the `--proto-types` flag unnecessary in most cases.

## Local development

#### Build binary

Run `make digger`, which will place a binary in `build/digger`.

#### Run unit tests

First, run `docker-compose up -d` to start up local Kafka and S3 endpoints. Then,
run the tests with `make test`.

When you're done running the tests, you can stop the Kafka and S3 containers by running
`docker-compose down`.
