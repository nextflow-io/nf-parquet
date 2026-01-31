include { countParquet } from 'plugin/nf-parquet'

channel.fromPath("data/*.parquet")
        .countParquet( )
        | view
