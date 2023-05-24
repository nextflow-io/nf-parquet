# nf-parquet

Nextflow plugin for interacting with [Apache Parquet](https://parquet.apache.org/) files.

## Usage

To use this plugin, simply specify it in your Nextflow configuration:

```groovy
plugins {
    id 'nf-parquet'
}
```

Or on the command line:

```bash
nextflow run <pipeline> -plugins nf-parquet
```

This plugin provides two new operators:

`splitParquet()`

: Load each Parquet file in a source channel, emitting each row as a separate item.

`toParquet( path )`

: Write each item in a source channel to a Parquet file.

## Development

Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for instructions on how to build, test, and publish Nextflow plugins.
