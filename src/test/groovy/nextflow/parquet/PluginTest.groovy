package nextflow.parquet

import com.jerolba.carpet.CarpetReader
import nextflow.Channel
import nextflow.plugin.Plugins
import nextflow.plugin.TestPluginDescriptorFinder
import nextflow.plugin.TestPluginManager
import nextflow.plugin.extension.PluginExtensionProvider
import org.pf4j.PluginDescriptorFinder
import spock.lang.Shared
import test.Dsl2Spec
import test.MockScriptRunner

import java.nio.file.Files
import java.nio.file.Path
import java.util.jar.Manifest

class PluginTest extends Dsl2Spec{

    @Shared String pluginsMode

    def setup() {
// reset previous instances
        PluginExtensionProvider.reset()
        // this need to be set *before* the plugin manager class is created
        pluginsMode = System.getProperty('pf4j.mode')
        System.setProperty('pf4j.mode', 'dev')
        // the plugin root should
        def root = Path.of('.').toAbsolutePath().normalize()
        def manager = new TestPluginManager(root){
            @Override
            protected PluginDescriptorFinder createPluginDescriptorFinder() {
                return new TestPluginDescriptorFinder(){
                    @Override
                    protected Manifest readManifestFromDirectory(Path pluginPath) {
                        def manifestPath= getManifestPath(pluginPath)
                        final input = Files.newInputStream(manifestPath)
                        return new Manifest(input)
                    }
                    protected Path getManifestPath(Path pluginPath) {
                        return pluginPath.resolve('build/tmp/jar/MANIFEST.MF')
                    }
                }
            }
        }
        Plugins.init(root, 'dev', manager)
    }

    def cleanup() {
        Plugins.stop()
        PluginExtensionProvider.reset()
        pluginsMode ? System.setProperty('pf4j.mode',pluginsMode) : System.clearProperty('pf4j.mode')
    }

    def 'should starts' () {
        when:
        def SCRIPT = '''
            channel.of('hi!') 
            '''
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == 'hi!'
        result.val == Channel.STOP
    }

    def 'should parse a parquet file in raw mode'(){
        when:
        def path = getClass().getResource('/test.parquet').toURI().path
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'
        channel.fromPath("$path").splitParquet() 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == [id:1, name:"test2", sizell:10, value:0.010838246310055144, percentile:0.28001529169191186]
        result.val == Channel.STOP
    }

    def 'should parse a projection'(){
        when:
        def path = getClass().getResource('/test.parquet').toURI().path
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        record SingleRecord(long id, String name) {
        }

        channel.fromPath("$path").splitParquet( [record:SingleRecord] ) 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val.id == 1
        result.val == Channel.STOP
    }

    def 'should write a projection to a file'(){
        when:
        def pathInput = getClass().getResource('/test.parquet').toURI().path
        def pathOutput = Files.createTempFile("", ".parquet")
        def SCRIPT = """
        include {splitParquet; toParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.of(1,2,3)
                .map( { new DemoRecord(it, "The \$it record") } )
                .toParquet("$pathOutput", [record:DemoRecord])
                .view()
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val.id == 1
        result.val.id == 2
        result.val.id == 3
        result.val == Channel.STOP
        pathOutput.toFile().length()
    }

    def 'should parse a parquet file in raw mode using by 1'() {
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path

        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'
        channel.fromPath("$path").splitParquet(by:1) 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == [id: 1, name: "The 1 record"]
        result.val == [id: 2, name: "The 2 record"]
        result.val == [id: 3, name: "The 3 record"]
        result.val == Channel.STOP
    }

    def 'should parse a parquet file in raw mode using by 2'(){
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'
        channel.fromPath("$path").splitParquet(by:2) 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == [ [id:1, name:"The 1 record"], [id:2, name:"The 2 record"] ]
        result.val == [ [id:3, name:"The 3 record"] ]
        result.val == Channel.STOP

    }

    def 'should chunk a parquet file using file as string'(){
        given:
        def dir = Files.createTempDirectory("")
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.fromPath("$path")
                .splitParquet(by:2, file:'${dir.toAbsolutePath()}', record:DemoRecord)
 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val instanceof Path
        result.val instanceof Path
        result.val == Channel.STOP

    }

    def 'should chunk a parquet file using file as boolean'(){
        given:
        def dir = Files.createTempDirectory("")
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.fromPath("$path")
                .splitParquet(by:2, file:true, record:DemoRecord)
 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val instanceof Path
        result.val instanceof Path
        result.val == Channel.STOP

    }

    def 'should chunk only 2 records from parquet file using limit'(){
        given:
        def dir = Files.createTempDirectory("")
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.fromPath("$path")
                .splitParquet(limit:2, by:2, file:true, record:DemoRecord)
 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val instanceof Path
        result.val == Channel.STOP

    }

    def 'should parse a parquet file specified in elem'(){
        given:
        def dir = Files.createTempDirectory("")
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.fromPath("$path")
                .map{ p -> [ 1, p] }
                .splitParquet( elem: 1 )
 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == [1, [id:1, name:'The 1 record']]
        result.val == [1, [id:2, name:'The 2 record']]
        result.val == [1, [id:3, name:'The 3 record']]
        result.val == Channel.STOP

    }

    def 'should transform the records of the parquet file'(){
        given:
        def dir = Files.createTempDirectory("")
        def path = getClass().getResource('/multiple.parquet').toURI().path
        when:
        def SCRIPT = """
        include {splitParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.fromPath("$path")                
                .splitParquet( each:{ r-> r.name } )
 
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        then:
        result.val == 'The 1 record'
        result.val == 'The 2 record'
        result.val == 'The 3 record'
        result.val == Channel.STOP

    }

    def 'should write #total records to a file using by #by '(){
        when:
        def pathOutput = Files.createTempFile("", ".parquet")
        def SCRIPT = """
        include {splitParquet; toParquet} from 'plugin/nf-parquet'

        import nextflow.parquet.DemoRecord

        channel.of(1..$total)
                .map( { new DemoRecord(it, "The \$it record") } )
                .toParquet("$pathOutput", [record:DemoRecord, by:$by])
                .view()
        """.toString()
        and:
        def result = new MockScriptRunner([:]).setScript(SCRIPT).execute()
        def values = (1..total).collect{ result.val }
        then:
        values.size() == total
        result.val == Channel.STOP

        when:
        final reader = new CarpetReader(pathOutput.toFile(), Map)
        int count = 0
        for (def record : reader) {
            println record
            count++
        }
        then:
        count == total

        where:
        by       | total
        1       | 100
        10      | 100
        100     | 100
    }
}
