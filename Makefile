all:
	mpirun --oversubscribe -n 50 python3 src/main.py 10 1
