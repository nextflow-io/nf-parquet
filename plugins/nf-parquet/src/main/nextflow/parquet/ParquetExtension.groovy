package nextflow.parquet

import java.nio.file.Path

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovyx.gpars.dataflow.DataflowReadChannel
import groovyx.gpars.dataflow.DataflowWriteChannel
import nextflow.Channel
import nextflow.Session
import nextflow.extension.CH
import nextflow.extension.DataflowHelper
import nextflow.plugin.extension.Operator
import nextflow.plugin.extension.PluginExtensionPoint
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory

/**
 * Implements extensions for reading and writing Parquet files.
 *
 * @author Ben Sherman <bentshermann@gmail.com>
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
     * Load each Parquet file in a source channel, emitting each row as a separate item.
     *
     * @param path
     */
    @Operator
    DataflowWriteChannel splitParquet(DataflowReadChannel source) {
        final target = CH.create()
        final splitter = new ParquetSplitter(target)

        final onNext = { it -> splitter.apply(it) }
        final onComplete = { target << Channel.STOP }
        DataflowHelper.subscribeImpl(source, [onNext: onNext, onComplete: onComplete])

        return target
    }

    class ParquetSplitter {
        private DataflowWriteChannel target

        ParquetSplitter(DataflowWriteChannel target) {
            this.target = target
        }

        void apply(Object source) {
            try {
                // create parquet reader
                final reader = ParquetFileReader.open(HadoopInputFile.fromPath(toHadoopPath(source), new Configuration()))
                final schema = reader.getFooter().getFileMetaData().getSchema()
                final fields = schema.getFields()

                // read each row from parquet file
                def pages = null
                try {
                    while( (pages=reader.readNextRowGroup()) != null ) {
                        final rows = pages.getRowCount()
                        final columnIO = new ColumnIOFactory().getColumnIO(schema)
                        final recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))

                        for( int i = 0; i < rows; i++ )
                            target << fetchRow(recordReader.read())
                    }
                }
                finally {
                    reader.close()
                }
            }
            catch( IOException e ) {
                throw new IllegalStateException("Error while reading Parquet file - cause: ${e.message ?: e}", e)
            }
        }

        private HadoopPath toHadoopPath(Object source) {
            if( source instanceof String )
                new HadoopPath((String)source)
            else if( source instanceof Path )
                new HadoopPath(((Path)source).toUriString())
            else
                throw new IllegalArgumentException("Invalid input for splitParquet operator: ${source}")
        }

        private Map fetchRow(Group group) {
            def result = [:]

            final fieldCount = group.getType().getFieldCount()
            for( int field = 0; field < fieldCount; field++ ) {
                final valueCount = group.getFieldRepetitionCount(field)
                final fieldType = group.getType().getType(field)
                final fieldName = fieldType.getName()

                for( int index = 0; index < valueCount; index++ )
                    if( fieldType.isPrimitive() ) {
                        println "${fieldName} ${group.getValueToString(field, index)}"
                        result[fieldName] = group.getValueToString(field, index)
                    }
            }

            return result
        }
    }

    /**
     * Write each item in a source channel to a Parquet file.
     *
     * @param source
     * @param path
     */
    @Operator
    DataflowReadChannel toParquet(DataflowReadChannel source, String path) {
        final onNext = {
            println it
        }
        DataflowHelper.subscribeImpl(source, [onNext: onNext])
        return source
    }

}
