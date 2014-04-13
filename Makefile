
build:
	@echo "Building"
	@ant
	@echo "Running"
	./run_dev.sh toy_example 

test: 
	@cd toy_example && ./grader.sh
