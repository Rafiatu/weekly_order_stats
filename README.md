# Weekly Customer Order Statistics


## Description
This is a basic ETL task in python that transforms daily customer orders into weekly customer orders. 
It reads the data from the specified excel sheet, performs groupBy and aggregate functions using pyspark.


## Getting Started

Setting up this project is pretty simple.

If you have previously cloned this repo, change directory into `weekly_order_stats` and simply update your local branch by running the command:
```
   git pull origin main --rebase
```
and continue from step 3.


1. Clone this repo using this command 
``` 
   git clone https://github.com/Rafiatu/weekly_order_stats.git
```


2. Change directory into `weekly_order_stats` to be able to run everything successfully.


3. You must have [Docker](https://www.docker.com/) installed on your PC in order to run this project on docker successfully. 
Once you have Docker up and running, build the base python image used for the project that was specified in ***dockerfile*** by running 
```
   docker build --tag task . 
``` 
*Note the dot (.) at end of the command*. This process may take a couple minutes especially with installing default-jdk and pyspark.
 

4. Once the build process is finished, you can run the python image that was just built using the command:
``` 
   docker run task
```


5. To run the main ETL functions and get the aggregated data from the provided daily orders data saved into a csv file, run the `main.py` script using the following command
``` 
   docker run task python main.py 
```

In summary, these are the lists of commands to run in order, to start up the project.
```
   1. git clone https://github.com/Rafiatu/weekly_order_stats.git
   2. cd weekly_order_stats
   3. docker build --tag task .
   4. docker run task python main.py
```

## Running Tests
This project is shipped with python unittest. Running the tests via docker is pretty straightforward.
In the terminal, run the command 
```
   docker run task python -m unittest 
```

## License

The MIT License - Copyright (c) 2022 - Rafihatu Bello
