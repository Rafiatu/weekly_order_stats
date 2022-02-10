# Weekly Customer Order Statistics


## Description
This is a basic ETL task in python that transforms daily customer orders into weekly customer orders. 
It reads the data from the specified excel sheet, performs groupBy and aggregate functions using pyspark.


## Getting Started

Setting up this project is pretty simple.
1. Clone this repo using this command 
``` 
   git clone https://github.com/Rafiatu/weekly_order_stats.git
```
   If you have previously cloned this repo, simply update your local branch by running the command:
   ```
      git pull origin main --rebase
   ```

2. Change directory into `weekly_order_stats` and create a virtual environment for this project using the following command:
``` 
   python -m venv venv 
```

3. This project uses [Docker](https://www.docker.com/). You must have docker installed on your PC in order to run this project on docker successfully.
Build the base python image used for the project that was specified in ***dockerfile*** by running ` docker build . ` *Note the dot (.) at end of the command*.
 

4. Once your virtual environment is activated, install the project's requirements by running this command in the terminal.
``` 
   pip install -r requirements.txt 
```

5. To run the main ETL functions and get the aggregated data saved into a csv file, 
you can go into `main.py` file of this project, then add this line of code:
``` 
orders.load_data_to_csv(<path_to_csv_file_you_want_the_data_saved>)
```
An example has been made available already in `main.py`.

6. Run the `main.py` script from the command line with the following command
``` 
   python main.py 
```

In summary, these are the lists of commands to run in listed order, to start up the project.
```
1. git clone https://github.com/decadevs/teamB-bouncer-api.git
2. cd teamB-bouncer-api
3. docker build .
4. docker-compose up
5. docker-compose exec api python project/manage.py makemigrations db
6. docker-compose exec api python project/manage.py migrate
```

## Running Tests
This project is shipped with python unittest. Running the tests is pretty straightforward.
In the terminal, run the command 
```
   python -m unittest 
```


## License

The MIT License - Copyright (c) 2022 - Rafihatu Bello
