rm -rf testbuild
mkdir testbuild
cd testbuild
go build -race ../mrworker.go
go build -race ../mrcoordinator.go
PROG=letter-count
go build -race -buildmode=plugin ../../mrapps/$PROG.go 
./mrcoordinator ../large_text_file.txt &
./mrworker $PROG.so &
./mrworker $PROG.so &
./mrworker $PROG.so &
wait 
sort mr-out-* | less
