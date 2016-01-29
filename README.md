# Tribbler
Tribbler is a course project in Distributed System class
## Functionality
1. Built a Twitter-like information dissemination service using RPC calls in Golang that allows users to post messages, read messages, and subscribe to receive other users' messages.
2. Implemented consistent hashing on back-end storage servers, and a lease-based cache consistency mechanism that cache frequently-accessed messages to improve the scalability of the system.
## Compiling the code
To and compile your code, execute one or more of the following commands (the resulting binaries will be located in the ```$GOPATH/bin``` directory.
```
go install github.com/cmu440/tribbler/runners/srunner
go install github.com/cmu440/tribbler/runners/lrunner
go install github.com/cmu440/tribbler/runners/trunner
go install github.com/cmu440/tribbler/runners/crunner
```
