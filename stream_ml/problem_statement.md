Context

IoT devices send out continuous data which we want to collect (e.g thermostat, heart rate meter, 
car fuel
readings, etc).


Your task is to build a realtime pipeline via which we can process the IoT data 
in a scalable manner. In
addition to that, we want to have a web service 
for querying the readings (e.g average/median/max/min
values) of specific sensors or groups of sensors for a specific timeframe.
We do not expect a production ready system, but it should be a good working prototype.


You may use any publicly available open source tools and libraries
Requirements

● Solution is written in Java, Scala and/or Python
● Simulate data for at least 3 IoT devices which send out a value every second
● Scalable and extendable to work with more IoT devices
● Fast
● Self-contained

Optional
You get the task to make a design of a extension on top of what you have already build.
The extension we
want to build is a pipeline in which we can run prediction models. 
How would that roughly look like in terms
of components and data flow and what are important factors to take into account in 
regards to the data?
Your submission should contain the following
● Source code
● Short description of the approach and limitations of the implementation
● Instructions on how to run and how to access the service