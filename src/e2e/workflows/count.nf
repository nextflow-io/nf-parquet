include { countParquet } from 'plugin/nf-parquet'

channel.fromPath("${baseDir}/data/presidents.parquet")
        .countParquet( )
        | view
