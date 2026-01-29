package nextflow.parquet

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.parquet.impl.ReadRecordAware

@CompileStatic
@Slf4j
class EmitterRecordChannel implements ReadRecordAware{

    private final DataflowWriteChannel target

    EmitterRecordChannel(DataflowWriteChannel target) {
        this.target = target
    }

    @Override
    void recordRead(Object row) {
        target << row
    }

    void complete(){
        target << Channel.STOP
    }
}
