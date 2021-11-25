### My 6.824 Lab

To build the plugin file to .so file successful, please `export GO111MODULE=off` first. 

Build in plugin mode can't use relative path, so when import a package, you have to write
the package path start from the $GOPATH(or $GOROOT).

#### Lab1 test-mr.sh problem
For test-mr.sh to test MapReduce result, the shell file will start different types of test, my code could pass all 
   test except the last crash test. It makes me so confused, and I check my code again and again, at last I find the
reason.

If you browse the test-mr.sh, you will find that it runs lots of threads by use timeout(or gtimeout on macOS),
 one thread for mrmaster.go and 2 or 3 thread for mrworker.go, at the end of a test, 
it will use wait command for the regular-exit of the thread, but it the number of wait it's just equal to the 
worker thread, not include master thread, but the exit of the master will occupy one wait command, this will 
make some previous worker thread still not exit, and their result may pollute the result of the new task.
The most simple way is to annotate the test before the last crash test in test-mr.sh, my code can pass the crash test
by doing this. My 6.824 course version is 2020 Spring, and in 2021 Spring version, they have fix the problem.