
config ?= compileClasspath

ifdef module 
mm = :${module}:
else 
mm = 
endif 

clean:
	./gradlew clean

compile:
	./gradlew :nextflow:exportClasspath compileGroovy
	@echo "DONE `date`"


check:
	./gradlew check


#
# Show dependencies try `make deps config=runtime`, `make deps config=google`
#
deps:
	./gradlew -q ${mm}dependencies --configuration ${config}

deps-all:
	./gradlew -q dependencyInsight --configuration ${config} --dependency ${module}

#
# Refresh SNAPSHOTs dependencies
#
refresh:
	./gradlew --refresh-dependencies 

#
# Run all tests or selected ones
#
test:
ifndef class
	./gradlew ${mm}test
else
	./gradlew ${mm}test --tests ${class}
endif

e2e:
	./gradlew installPlugin -Pversion=99.99.99
	cd src/e2e; PARQUET_PLUGIN_VERSION=99.99.99 ./nf-test test ${test}

#
# Install last version into $HOME/.nextflow/plugins
#
installPlugin:
	./gradlew installPlugin

