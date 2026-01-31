include { splitParquet } from 'plugin/nf-parquet'

record SingleRecord(long id, String name) {
}

channel.fromPath("data/test*.parquet").splitParquet( record: SingleRecord)
        | view
