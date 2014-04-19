
build:
	@echo "Building"
	@ant
	@echo "Running"
	./run_dev.sh task1 

test: 
	@ant
	@cd toy_example && rm -rf output/index && ./grader.sh
