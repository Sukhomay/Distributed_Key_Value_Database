make:
	g++ -std=c++20 -c lsm.cpp -o lsm.o -pthread
	gcc -c server.c -pthread
	g++ -std=c++20 server.o lsm.o -o server -pthread
	
clean:
	rm -f *.o
	rm -rf SSTable_*