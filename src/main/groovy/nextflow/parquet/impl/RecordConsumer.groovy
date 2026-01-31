package nextflow.parquet.impl

interface RecordConsumer {

    boolean wantMore(Object row )

}
