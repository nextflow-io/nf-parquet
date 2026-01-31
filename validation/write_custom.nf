include { splitParquet; toParquet } from 'plugin/nf-parquet'

import myrecords.*

channel.of(1..1_673)
        .map( {
            new CustomRecord(it, "the $it record", it, it, it)
        })
        .toParquet("work/demo.parquet",[record: CustomRecord, by:100])
        | view
