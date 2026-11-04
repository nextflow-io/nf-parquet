include { splitParquet } from 'plugin/nf-parquet'

import myrecords.CustomRecord

channel.fromPath("${baseDir}/data/customs.parquet")
        .splitParquet(by:500, file: true, record:CustomRecord)
        | map{ it.name }
        | view
