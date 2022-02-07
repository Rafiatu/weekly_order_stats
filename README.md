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

2. Change directory into `weekly_order_stats` and create a virtual environment for this project using the following command:
``` 
   python -m venv venv 
```

3. Activate the virtual environment you just created in step 2 using either of the following commands
   - On Windows: ` venv\Scripts\activate `
   - On Mac: ` source venv/bin/activate `

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


## Running Tests
This project is shipped with python unittest. Running the tests is pretty straightforward.
In the terminal, run the command 
```
   python -m unittest 
```


## License

The MIT License - Copyright (c) 2022 - Rafihatu Bello
