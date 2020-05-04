# Davis-base-lite

Rudimentary database engine that is loosely based on a
hybrid between MySQL and SQLite using file-per-table approach. 

Developed as a project for Database design course by Dr.Chris Davis.

## Supported Operations

- CREATE TABLE
- INSERT INTO TABLE
- SELECT FROM TABLE

## Software requirements

- Java 11+
- Apache Ant 1.10.7+

## To build

- Open command prompt in project directory
- Run the command 
`ant -f davis-base-lite.xml`

## To run

- Build the project
- Open command prompt in the build/lib directory
- Run the command
`java -jar davis-base-lite-1.0.jar`

## To test

- Enter command `devtest;` in davisql prompt 
Command creates `dummy` table with random values