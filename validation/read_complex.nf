include { splitParquet } from 'plugin/nf-parquet'

channel.fromPath("data/presidents.parquet").splitParquet()
        | view
