# CloudDBSOD
A client that find an optimal Separation of Duties for Cloud Databases.

## Cite as

- Ferdinand Bollwein, Lena Wiese: Keeping Secrets by Separation of Duties While Minimizing the Amount of Cloud Servers. T. Large-Scale Data- and Knowledge-Centered Systems 37: 1-40 (2018)
- Ferdinand Bollwein, Lena Wiese: On the Hardness of Separation of Duties Problems for Cloud Databases. TrustBus 2018: 23-38 
- Ferdinand Bollwein, Lena Wiese: Closeness Constraints for Separation of Duties in Cloud Databases as an Optimization Problem. BICOD 2017: 133-145
- Ferdinand Bollwein, Lena Wiese: Separation of Duties for Multiple Relations in Cloud Databases as an Optimization Problem. IDEAS 2017: 98-107

## Installation instructions

This application depends on three third-party libraries:
- The PostgreSQL JDBC Driver (https://jdbc.postgresql.org/)
- The JsqlParser (https://github.com/JSQLParser/JSqlParser)
- The IBM ILOG CPLEX Java library (http://www-03.ibm.com/software/products/de/ibmilogcpleoptistud)

To build an running .jar file, one has to specify the necessary dependencies in the build.xml:
- The path to the cplex.jar must be specified
- The path to the jsqlparser.jar needs to be specified
- The path to the postgresql.jar needs to be specified

Subsequently, running
```
ant jar
```
builds the executable SoD.jar file.

## Execution

Before executing the program, the available servers must be stated in the configuration file "./config/config.txt". The capacity is measured in MB.

To state the parameters for the Separation of Duties Problem, a folder with the database's name must be created in ./fragmentations. In this folder, the files "confidentiality.txt", "dependency.txt", "visibility.txt" and "closeness.txt" can be created to introduce the constraints and dependencies. (for more information, see the hospital example)

The database to be fragmented must be present at the trusted database server.

To set up or use a distributed database, SoD.jar must be executed. If a distributed database should be set up, the CPLEX library has to be linked. For a standard installation of CPLEX 12.7, this can be done with the parameters
```
-Djava.library.path=/opt/ibm/ILOG/CPLEX_Studio127/cplex/bin/x86-64_linux
```
on Linux or
```
-Djava.library.path="C:\Program Files\IBM\ILOG\CPLEX_Studio127\cplex\bin\x64_win64"
```
on Windows.
Alternatively, these paths can be added to the LD_LIBRARY_PATH variable on Linux or to the PATH variable on Windows.

After the execution of the program, the command line guides through the necessary steps to set up and to query the distributed database.

Additionally, if the Program is run with the parameters -b tpch or -b tpce, the TPC-H or the TPC-E benchmark is executed.

Typing `exit` or `exit;` stops the program.
