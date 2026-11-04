include { splitParquet } from 'plugin/nf-parquet'

channel.fromPath("${baseDir}/data/presidents.parquet").splitParquet()
        | view
