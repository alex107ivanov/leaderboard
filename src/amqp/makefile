CC=g++
CFLAGS=-c -Wall -I/usr/local/include/ -std=c++11 -g
LDFLAGS=-L/usr/local/lib64/ -lrabbitmq -lboost_system -lboost_thread -lredox -lhiredis -lev
SOURCES=leaderboard.cpp AMQP.cpp RedisConnection.cpp split.cpp
OBJECTS=$(SOURCES:.cpp=.o)
EXECUTABLE=leaderboard

all: $(SOURCES) $(EXECUTABLE)

$(EXECUTABLE): $(OBJECTS) 
	$(CC) $(LDFLAGS) $(OBJECTS) -o $@

.cpp.o:
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -rf *.o $(EXECUTABLE)
