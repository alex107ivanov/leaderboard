CC=g++
CFLAGS=-c -Wall -I/usr/local/include/ -std=c++11 -g
LDFLAGS=-L/usr/local/lib64/ -lrabbitmq -lboost_system -lboost_thread -lredox -lhiredis -lev -lpthread -lprotobuf
SOURCES=leaderboard.cpp AMQP.cpp RedisConnection.cpp split.cpp AMQPConnection.cpp protocol.pb.cc UserInfoRequester.cpp
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=leaderboard

all: protobuf $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(OBJECTS) $(LDFLAGS) -o $@

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *.o $(EXECUTABLE)

protobuf:
	protoc --cpp_out=. --python_out=. protocol.proto
