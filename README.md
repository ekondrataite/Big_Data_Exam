# Big Data Exam Task

## **The objective** 
To find the two closest moving vessels in a particular sea area and visualize their trajectory from 10 minutes before rendezvous time to 10 minutes after.

## **Subtasks to complete the objective**
- To complete this task Python and PySpark were used.
- Firstly, a function to process each day csv file was created. It does the following:
  - Performs Data Preparation:
    - Selects the needed columns, ensures correct data types;
    - Ensures logical values for Latitude and Longitude;
  - Performs Data Processing:
    - Finds all vessels in the area where the circle center coordinate is Latitude: 55.225000, Longitude: 14.245000;
    - Finds the clossest two vessels in the desired area;
    - Collects the information about each vessels trajectory 10 minutes before the rendezvous time and 10 after;
- Secondly, the approach to process each file containing daily vessel data from December 1, 2021, to December 31, 2021:
  - Each csv file is extracted to a temporary directory;
  - Each file is processed using the previously discussed function and a **for cicle**;
  - The results of each day clossest vessels and their trajectories are collected into one dataframe for further analysis.
- Thirdly, the result is processed and we find the most clossest vessel pair when comparing clossest vessel pairs from each day.
- Finally, the trajectories of the clossest vessels are visualized.

## **The result**
The clossests vessels were **266334000** and **265388000**. The day on which they were clossest is **December 13, 2021**.

To see the result follow this link: https://rawcdn.githack.com/ekondrataite/Big_Data_Exam/90149c520f407f17ed832de97973356c519acadb/result_of_vessels_trajectories.html
