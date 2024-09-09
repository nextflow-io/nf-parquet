include { splitParquet; toParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.of(1,2,3)
        .map( {
            new CustomRecord(it, "the $it record", it, it, it)
        })
        .toParquet("work/demo.parquet", [record: CustomRecord])
        | view