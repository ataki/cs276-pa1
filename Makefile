
build:
	@echo "Building"
	@ant
	@echo "Running"
	./run_dev.sh toy_example 

test: 
	@ant
	@cd toy_example && rm -rf output/index && ./grader.sh
