package nextflow.extension

import groovy.transform.CompileDynamic
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.splitter.AbstractSplitter
import nextflow.splitter.FastqSplitter
import nextflow.splitter.ParquetSplitter

@CompileDynamic // Mandatory dynamic to avoid issues between plugin and core classLoaders
class SplitOpExt extends SplitOp{

    /**
     * Creates a splitter operator
     *
     * @param source The source channel to which apply to operator
     * @param methodName The operator method name eg. {@code splitFasta}, {@code splitCsv}, etc.
     * @param opts The operator named options
     */
    SplitOpExt(DataflowReadChannel source, String methodName, Map opts) {
        super(source, methodName, opts)
    }

    @Override
    protected DataflowWriteChannel splitSingleEntry(DataflowReadChannel origin, Map params) {
        final output = getOrCreateWriteChannel(params)
        // -- the output channel is passed to the splitter by using the `into` parameter
        params.into = output

        // -- create the splitter and set the options
        def splitter = createSplitter(methodName, params)

        // -- specify if it's a multi-file splitting operation
        if( multiSplit )
            splitter.multiSplit = true

        // -- specify if it's a paired-end fastq splitting operation
        if( pairedEnd )
            (splitter as FastqSplitter).emitSplitIndex = true

        // -- finally apply the splitting operator
        applySplittingOperator(origin, output, splitter)
        return output
    }

    @Override
    AbstractSplitter createSplitter(String methodName, Map params) {
        if( ["split", "count"].contains(methodName))
            return new ParquetSplitter(methodName).options(params)
        throw new IllegalArgumentException("Unknow ${methodName} method")
    }
}
