# the compiler: gcc for C program, define as g++ for C++
CC = g++

# compiler flags:
#  -g     - this flag adds debugging information to the executable file
#  -Wall  - this flag is used to turn on most compiler warnings
# CFLAGS  = -g -Wall -Ofast -Wno-stringop-overflow -march=native -mtune=native -flto=auto -funroll-loops -fno-plt #-O3 #-D_DEBUG
CFLAGS = -g -Wall -D_DEBUG

AGC_CFLAGS = -Wall -g -DNO_INCLUDE_FENV
LDFLAGS = -lstdc++ -llmdb

# The build target 
ESEMAN_SERVER = eseman_data_server
INCLUDE = -I./rapidjson/include

AGC = agglomerate_clustering
ESEMAN = eseman_kdt

DGEM_ID = 589ca754-ef75-426c-8d51-841cc61dc84a
KMEANS_ID = 8b3289c9-a740-4091-a56d-e4d55af526b5
LULESH_ID = 772c7330-d4eb-485b-866a-3b315063f9af
KMENAS_LARGE_ID = c3d5e8fe-32df-4f4f-8cbb-4ba6fabd7d3d
DATASET_ID ?= $(KMEANS_ID)

all: $(ESEMAN_SERVER).cpp $(AGC).cpp $(ESEMAN).cpp fastcluster.o
	$(RM) $(ESEMAN_SERVER)
	$(CC) $(CFLAGS) -o $(ESEMAN_SERVER) $(ESEMAN_SERVER).cpp $(CGET).cpp $(OBKDT).cpp $(AGC).cpp $(ESEMAN).cpp fastcluster.o $(INCLUDE) -lcurl -llmdb
#	./$(ESEMAN_SERVER) $(DATASET_ID) false

$(AGC): $(AGC).cpp fastcluster.o
	$(RM) $(AGC)
	$(CC) -D_DEBUG -g -Wall -o $(AGC) $(AGC).cpp fastcluster.o
	./$(AGC)

fastcluster.o: hclust-cpp/fastcluster.cpp hclust-cpp/fastcluster.h
	$(CC) $(AGC_CFLAGS) $(CPPFLAGS) -c hclust-cpp/fastcluster.cpp

$(ESEMAN): $(ESEMAN).cpp $(ESEMAN).h
	$(RM) $(ESEMAN)
	$(CC) -D_DEBUG -DTESTING -g -Wall -o $(ESEMAN) $(ESEMAN).cpp
	./$(ESEMAN)

clean:
	$(RM) $(TARGET) $(CLIENT) $(CGET) $(OBKDT) $(AGC).o $(AGC)
