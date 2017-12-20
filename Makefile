all:
	python3 src/generator.py 300000 0
	mpirun --oversubscribe -n 50 python3 src/main.py 10000 1
