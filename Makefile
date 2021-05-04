all:
	go run .

build:
	go build -o client.exe

clean:
	rm -rf tmp/* *.db *.exe

race:
	go build -race -o client.exe

master:
	go run . -master -wait data/austen.db out.db

part1:
	go run . -mode part1

part2:
	go run . -mode part2
