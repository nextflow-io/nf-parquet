include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("data/customs.parquet")
        .splitParquet(by:500, file: true, record:CustomRecord)
        | view
