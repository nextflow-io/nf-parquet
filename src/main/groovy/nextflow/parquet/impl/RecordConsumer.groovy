package nextflow.parquet.impl

interface RecordConsumer {

    boolean next(Object row )

}
