package nextflow.parquet

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.extension.SplitOpExt
import nextflow.parquet.impl.ParquetWriter
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint
import nextflow.splitter.ParquetSplitter
import nextflow.splitter.SplitterFactory

/**
 * Implements extensions for reading and writing Parquet files.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
 * @author Jorge Aguilera <jorge@edn.es>
 */
@Slf4j
@CompileStatic
class ParquetExtension extends PluginExtensionPoint {

    private Session session

    @Override
    protected void init(Session session) {
        this.session = session
    }

    /**
     * Load a Parquet file emitting each row as a separate item.
     *
     * @param path
     */
    @Operator
    DataflowWriteChannel splitParquet(DataflowReadChannel source, Map params=[:]) {
        def operator = new SplitOpExt(source, "split", params)
        operator.apply()
    }

    @Operator
    DataflowWriteChannel countParquet(DataflowReadChannel source, Map params=[:]) {
        def splitter = new ParquetSplitter()
        SplitterFactory.countOverChannel(source, splitter, params)
    }


    /**
     * Write each item in a source channel to a Parquet file.
     *
     * @param source
     * @param path
     */
    @Operator
    DataflowWriteChannel toParquet(DataflowReadChannel source, String path, Map params=[:]) {
        final target = CH.createBy(source)
        final writer = new ParquetWriter(path, params)
        final onNext = {
            writer.write(it as Record)
            target << it
        }
        final onComplete = {
            writer.close()
            target << Channel.STOP
        }
        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])
        return target
    }

}
