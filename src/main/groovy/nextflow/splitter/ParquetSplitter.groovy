package nextflow.splitter

import groovy.transform.CompileStatic
import groovy.transform.InheritConstructors
import groovy.util.logging.Slf4j
import nextflow.Global
import nextflow.Nextflow
import nextflow.extension.FilesEx
import nextflow.parquet.impl.ParquetReader
import nextflow.parquet.impl.RecordConsumer
import nextflow.util.CacheHelper

import java.nio.file.Files
import java.nio.file.Path

@Slf4j
@CompileStatic
@InheritConstructors
class ParquetSplitter extends AbstractSplitter<File> {

    protected boolean fileMode

    protected Path collectPath

    protected String collectName

    protected Class<?>clazz

    private long itemsCount

    @Override
    protected Map<String, Object> validOptions() {
        def map = super.validOptions()
        map.remove('decompress')
        map.put('record', Class<Record>)
        map.put('file', [File, CharSequence, Path, Boolean])
        map
    }

    @Override
    AbstractSplitter options(Map options) {
        super.options(options)
        if( options.file instanceof Boolean )
            fileMode = options.file as Boolean
        else if( options.file instanceof Path ) {
            collectPath = (Path)options.file
            fileMode = true
        }
        else if( options.file instanceof CharSequence ) {
            collectName = options.file.toString()
            fileMode = true
        }
        this.clazz = options.containsKey('record') ? options.record as Class<Record> : null
        this
    }

    protected File normalizeSource( obj ) {

        if( obj instanceof File )
            return (File)obj

        if( obj instanceof InputStream ) {
            def path = Files.createTempFile("",".parquet")
            path.deleteOnExit()
            Files.copy((InputStream) obj, path)
            sourceFile = path
            return path.toFile()
        }

        if( obj instanceof byte[] ) {
            def bais= new ByteArrayInputStream((byte[]) obj)
            def path = Files.createTempFile("",".parquet")
            path.deleteOnExit()
            Files.copy(bais, path)
            sourceFile = path
            return path.toFile()
        }

        if( obj instanceof CharSequence ){
            def bais= new ByteArrayInputStream(obj.toString().bytes)
            def path = Files.createTempFile("",".parquet")
            path.deleteOnExit()
            Files.copy(bais, path)
            sourceFile = path
            return path.toFile()
        }

        if( obj instanceof Path && (obj as Path).toUriString().startsWith("s3://") ) {
            def path = Files.createTempFile("",".parquet")
            path.deleteOnExit()
            def source = obj as Path
            FilesEx.copyTo(source, path)
            sourceFile = path
            return path.toFile()
        }

        if( obj instanceof Path )
            return ((Path)obj).toFile()

        throw new IllegalAccessException("Object of class '${obj.class.name}' does not support 'splitter' methods")
    }

    boolean limitReached(){
        limit > 0 && ++itemsCount == limit
    }

    @Override
    protected Object process(File targetObject){
        assert targetObject != null
        def result = null

        try {
            final aware = new RecordConsumer() {
                @Override
                boolean next(Object record ) {
                    result = processChunk( record )
                    if (limitReached())
                        return true
                    false
                }
            }
            counter.reset()
            itemsCount = 0

            def reader = new ParquetReader(aware, [record: clazz])
            reader.readFile(sourceFile.toFile())

            if (collector && collector.hasChunk()) {
                result = invokeEachClosureExt(closure, collector.nextChunk())
            }
        }finally {
            if( collector instanceof Closeable )
                collector.close()
        }
        result
    }

    final invokeEachClosureExt( Closure closure, Object chunk ) {

        def result
        if( targetObj instanceof List ) {
            result = new ArrayList((List)targetObj)
            result.set(elem, chunk)
        }
        else {
            result = chunk
        }

        if( closure ) {
            result = closure.call(result)
        }

        if( into != null )
            append(into,result)

        return result
    }

    protected processChunk( record ) {

        def result = null

        if ( isCollectorEnabled() ) {
            collector.add(record)

            if( counter.isChunckComplete() ) {
                result = invokeEachClosureExt(closure, collector.nextChunk())
                counter.reset()
            }
        }
        else {
            result = invokeEachClosureExt(closure, record)
        }

        return result
    }

    protected boolean isCollectorEnabled() {
        return (counter.isEnabled() || fileMode)
    }

    @Override
    protected CollectorStrategy createCollector() {

        if( !isCollectorEnabled() )
            return null

        if( fileMode ) {
            def base = getCollectorBaseFile()
            return new ParquetFileCollector(base, clazz)
        }

        return new ObjectListCollector()
    }

    ParquetFileCollector.CachePath getCollectorBaseFile () {

        final fileName = getCollectFileName()
        log.trace "Splitter collector file name: $fileName"
        ParquetFileCollector.CachePath result
        if( collectPath ) {
            final file = sourceFile ?: targetObj
            final path = collectPath.isDirectory() ? collectPath.resolve(fileName) : collectPath
            final keys = [Global?.session?.uniqueId, file, getCacheableOptions()]
            final hash = CacheHelper.hasher(keys).hash()
            result = new ParquetFileCollector.CachePath(path,hash)
        }
        else if( sourceFile ) {
            final path = Nextflow.cacheableFile( [sourceFile, getCacheableOptions()], fileName)
            result = new ParquetFileCollector.CachePath(path)
        }
        else {
            final path = Nextflow.cacheableFile( [targetObj, getCacheableOptions()], fileName )
            result = new ParquetFileCollector.CachePath(path)
        }
        log.debug "Splitter `$operatorName` collector path: $result"
        result
    }

    protected Map getCacheableOptions() {
        def result = new HashMap( fOptionsMap )
        result.remove('into')
        result.remove('each')
        return result
    }


    protected String getCollectFileName() {
        def suffix = ".parquet"
        if( collectName ) {
            return multiSplit ? "${collectName}_${elem}${suffix}" : "${collectName}${suffix}"
        }

        if( sourceFile ) {
            def fileName = sourceFile.getName()
            return fileName
        }

        return multiSplit ? "chunk_$elem${suffix}" : "chunk${suffix}"
    }
}
