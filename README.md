# TP - Cloud and Edge infrastructures

## Students: Ignacio Perez (@NachitoPerez) and Diego Sierra (@dasierra2021)

The current project is to generate a web cloud architecture for a company to track its sales. The architecture consists of three different applications as follows:

* CLIENT application: uploads a CSV file containing the daily sales of an individual store
* WORKER application: reads the CSV files and summarizes the results of the daily sales by store and by product in another CSV file.
* CONSOLIDATOR applicaiton: grabs the last CSV file and summarizes the results for the whole company.         
