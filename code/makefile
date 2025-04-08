comp:
	g++ storage_node/replica_machine.cpp -o storage_node/replica_machine.out
	g++ storage_node/job_manager.cpp -o storage_node/job_manager.out
	g++ request_manager/request.cpp -o request_manager/request.out
run_job1:
	./storage_node/job_manager.out 0 7000
run_job2:
	./storage_node/job_manager.out 1 7001
run_job3:
	./storage_node/job_manager.out 2 7002
run_req:
	./request_manager/request.out
clean:
	find . -type f -name "*.out" -exec rm -f {} +