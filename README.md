### My 6.824 Lab

To build the plugin file to .so file successful, please `export GO111MODULE=off` first. 

Build in plugin mode can't use relative path, so when import a package, you have to write
the package path start from the $GOPATH(or $GOROOT).
