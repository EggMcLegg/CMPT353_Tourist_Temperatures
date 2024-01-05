# cmpt353project-TouristTemperatures
Project for CMPT353.  

Hypothesis: Weather in popular tourist locations is more mild than extreme.

Required extensions: 

  Pyspark and its requirements
  Pandas
  Numpy
  scipy.stats
  Pathlib
  glob

To run this program correctly, first run "pyspark", then "spark-submit process_data.py weather-3". Then run "python3 analysis.py popular_labelled_output all_labelled_output output" to get these same results.

process_data.py will create the processed_data folder, complete with the city-coords, city_counts.csv, ghcnd-countries.csv, and ghcnd-stations.csv. 

analysis.py will create the output file.

****************************************
This project was completed on August 4th, 2023 for CMPT353 - Computational Data Science at Simon Fraser University. 
