package nextflow.splitter

import nextflow.Channel
import nextflow.Nextflow
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.Path

class ParquetSplitterSpec extends Specification{

    def "should read a Path file"(){
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path

        when:
        def count=0
        def result = new ParquetSplitter().options(each:{ count++; it }).target(Path.of(path)).channel()

        then:
        count == 3
        result.val == [id: 1, name: "The 1 record"]
        result.val == [id: 2, name: "The 2 record"]
        result.val == [id: 3, name: "The 3 record"]
        result.val == Channel.STOP
    }

    def "should read a string file"(){
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path

        when:
        def count=0
        def result = new ParquetSplitter().options(each:{ count++; it }).target(path).channel()

        then:
        count == 3
        result.val == [id: 1, name: "The 1 record"]
        result.val == [id: 2, name: "The 2 record"]
        result.val == [id: 3, name: "The 3 record"]
        result.val == Channel.STOP
    }

    def "should read a parquet file by n"(){
        given:
        def path = getClass().getResource('/multiple.parquet').toURI().path

        when:
        def count=0
        def result = new ParquetSplitter().options(by:2, each:{ count++; it }).target(path).channel()

        then:
        count == 2
        result.val == [ [id: 1, name: "The 1 record"], [id: 2, name: "The 2 record"]]
        result.val == [ [id: 3, name: "The 3 record"] ]
        result.val == Channel.STOP
    }

}
