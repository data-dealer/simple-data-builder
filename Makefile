build-test-env:
	curl -o jars/deequ-2.0.3-spark-3.3.jar https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.3-spark-3.3/deequ-2.0.3-spark-3.3.jar
	docker build -f Dockerfile -t sdb-docker-env .

test:
	docker run --rm -it  \
	-v ${PWD}:/opt/workspace \
	sdb-docker-env pytest tests/ --capture=no --log-cli-level=ERROR

gendoc:
	cd src/ && pdoc --html sdb/ --http localhost:8002