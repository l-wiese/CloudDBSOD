Before executing the program, the available servers must be stated in the configuration file "./config/config.txt". The capacity is measured in MB.

To state the parameters for the Separation of Duties Problem, a folder with the database's name must be created in ./fragmentations. In this folder, the files "confidentiality.txt", "dependency.txt", "visibility.txt" and "closeness.txt" can be created to introduce the constraints and dependencies. (for more information, see the hospital example)

The database to be fragmented must be present at the trusted database server.

To set up or use a distributed database, SoD.jar must be executed. If a distributed database should be set up, the CPLEX library has to be linked. For a standard installation of CPLEX 12.7, this can be done with the parameters
	-Djava.library.path=/opt/ibm/ILOG/CPLEX_Studio127/cplex/bin/x86-64_linux
on Linux or
	-Djava.library.path="C:\Program Files\IBM\ILOG\CPLEX_Studio127\cplex\bin\x64_win64"
on Windows.
Alternatively, these paths can be added to the LD_LIBRARY_PATH variable on Linux or to the PATH variable on Windows.

After the execution of the program, the command line guides through the necessary steps to set up and to query the distributed database.

Additionally, if the Program is run with the parameters -b tpch or -b tpce, the TPC-H or the TPC-E benchmark is executed.

Typing "exit" or "exit;" stops the program.
