include { splitParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.fromPath("${baseDir}/data/presidents.parquet").splitParquet(by:1)
        | view
