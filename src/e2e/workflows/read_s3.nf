include { splitParquet } from 'plugin/nf-parquet'

channel.fromPath( "s3://indian-supreme-court-judgments/metadata/parquet/year=2026/metadata.parquet" )
        .splitParquet()
        | map{ it.title }
        | view
