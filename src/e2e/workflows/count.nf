include { countParquet } from 'plugin/nf-parquet'

url = params.url ?: "${baseDir}/data/presidents.parquet"

channel.fromPath(url)
        .countParquet( )
        | view
