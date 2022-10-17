# Nate Hausfater
UIN: 672990016
email: hausfat1@uic.edu

video:
[https://www.youtube.com/watch?v=WAFVdUa44UQ&list=PLpCHxrtVwRiAlmRMg7TMhdqpbBG3VcQVP&index=1
](https://www.youtube.com/watch?v=WAFVdUa44UQ&list=PLpCHxrtVwRiAlmRMg7TMhdqpbBG3VcQVP&index=1)


## Compiling the Code
The project code should be rather simple to compile. It may not even be necessary as a JAR file is
included that can be directly run on HADOOP, either local or AWS. However, if you wish to build
and compile locally, simply open the project in IntelliJ and build. The project must be
opened as an SBT project. Then simply `compile` in the SBT console and run `test`. The 
project should work smoothly. If you wish to generate your own JAR, first go to the `application.conf`
file and change the start time, end time, and interval to the values you wish. Secondly, set the number of mapper
and reducers to the desired number. If the JAR is to be run on AWS set `Local` to "0". Then
enter the commands `clean` and `compile` followed by `assembly` in the SBT console.

## Running Code
Present in the Github repository, directly in the 'HomeWork1' folder, is a file called
`hausfat1-HomeWork1_1.0-release.jar`. This is the JAR file which will be loaded in 
either a local HADOOP instance or AWS instance. However, be aware that the JAR file 
present is configured to run with the `logFileBig.log` logfile present under 
`HomeWork1/src/main/resources/logFileBig.log`.
    In order to change this, you will have to open the `application.conf` file located
in `HomeWork1/src/main/resources/application.conf` and change the `StartTime` and `EndTime`
values to reflect the desired values in your logfile. If these are not changed Mapper-Reducer
types 1 and 2 will not work properly. In order to run a type 2 Map-Reduce you must set the 
start and end times in `application.conf` to the first and last log entry times respectively.
In order to run a type 1 Map-Reduce, you must set the start and end times to the 
interval you wish to search. This interval must be within the range of the log 
file.

In order to run the program from within an SBT shell, first run `compile` and then 
enter `run location/of/logfile loction/you/wish/files/to/be/output mode`, where
the first location is the location of the logfile, and the second location is the 
destination and name of the folder you would like the output to be written to, and `mode` is a
digit `1`, `2`, `3`, or `4` representing Map-Reduce type 1, 2, 3, or 4. Thus the `run` command
must be composed of an input, output, and mode or three arguments.

The type 1 Mapper-Reducer corresponds to the functionality requirement "First, you will
compute a spreadsheet or an CSV file that shows the distribution of different types of messages across
predefined time intervals and injected string instances of the designated regex pattern for these log message
types." This is completed by searching the time frame between StartTime and EndTime in the
application.conf and tabulating the number of messages with regex strings by type.

The type 2 Mapper-Reducer corresponds to the functionality requirement "Second, you will compute
time intervals sorted in the descending order that contained most log messages of 
type ERROR with injected regex pattern string instances." This is done by taking time-slices
of size equal to `Interval` in application.conf and tabulating the number of error messages with
regex strings in each time slice.

The type 3 Mapper-Reducer corresponds to the functionality requirement "Then, for each message type you
will produce the number of the generated log messages." This is done by simply tabulating
all message types in the log.

The type 4 Mapper-Reducer corresponds to the functionality requirement "Finally, you will produce the number of 
characters in each log message for each log message type that contain the highest number of characters in the detected
instances of the designated regex pattern." This is accomplished by summing up the string size for
each message type with detected regex pattern, and then reducing to the number that is largest for each
message type.