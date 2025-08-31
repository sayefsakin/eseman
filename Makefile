# the compiler: gcc for C program, define as g++ for C++
CC = g++

# compiler flags:
#  -g     - this flag adds debugging information to the executable file
#  -Wall  - this flag is used to turn on most compiler warnings
# CFLAGS  = -g -Wall -Ofast -Wno-stringop-overflow -march=native -mtune=native -flto=auto -funroll-loops -fno-plt #-O3 #-D_DEBUG
CFLAGS = -g -Wall -D_DEBUG -march=native

AGC_CFLAGS = -Wall -g -DNO_INCLUDE_FENV
LDFLAGS = -lstdc++ -llmdb

# The build target 
ESEMAN_SERVER = eseman_data_server
INCLUDE = -I./rapidjson/include

AGC = agglomerate_clustering
ESEMAN = eseman_kdt

all: $(ESEMAN_SERVER).cpp $(AGC).cpp $(ESEMAN).cpp fastcluster.o
	$(RM) $(ESEMAN_SERVER)
	$(CC) $(CFLAGS) -o $(ESEMAN_SERVER) $(ESEMAN_SERVER).cpp $(AGC).cpp $(ESEMAN).cpp fastcluster.o $(INCLUDE) -llmdb
# 	./$(ESEMAN_SERVER) -b -i /mnt/d/all_traveler_otf2_files/all_data/json_data/faf17535-2f66-4621-995f-49c7dbd84e8b.json
	./$(ESEMAN_SERVER) -s

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
