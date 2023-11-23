# MapReduce Go Base Repository
## Installation
We assume you have installed Golang already on your system. For Golang installation instructions, visit their website here: https://golang.org/doc/install
Run install.sh on your system after Golang has been installed. The install script should work for debian distributions of linux.

The install script will do the following:
1. Install kubectl
2. Install protobuf
3. Install Kind
4. Install Helm
5. Install gRPC
6. Set up Go module with dependencies included

After set up, remember to add the path of Go and its libraries to the PATH environmental variables.
Do this by pasting the following into ~/.bash_profile or ~/.bashrc, depending on your environment.
``` bash
export PATH=$PATH:/usr/local/go/bin
export PATH="$PATH:$(go env GOPATH)/bin"
```
Let us know if you have any issues.

## Directory Structure
After installation, there should be a go.mod file. This defines the local go module for your implementation of MapReduce. The name of the module is defined as mapreduce,
and the dependencies are defined in the require section. 

The code for your master and worker files are in the cmd/ directory, in the master and worker packages.

Happy Coding :)
