# Chicago Crime Analysis

## Description of the repository

- A jupyter notebook.
- A link to the dataset found [here](https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD).
- Helper files for database operation and classes (see helpers folder)

## Business Understanding

This project is based on the dataset of the Crimes of Chicago from 2001 to Present, based on the Cross-Industry Standard Process of Data Mining (CRISP-DM), the repository answers 4 questions of:

- Which type of crimes are the mos frequent?  
- Which are the days that most of the crimes have been done?  
- Which is the day of most incidents?
- Can we summarize in a better graph?

## Data Understanding (Access and Explore)

Exploration of the dataset is done when we perform different queries, those queries are related to the start datetime and end datetime.  

Later we explore each dataset and make insights about the four questions above, in abrief, all the incidents occurs over 12PM and are related to several avenue crimes that are the most frequent ones incrementally between starting the endo of the week time.

## Data Preparation (Cleaning)

We perform data preparation for the entire dataset by cleaning unfilled values due to they are a small part of the dataset and also rename reduntant description of crimes.

Due to the majority of the dataset is near 7M of data points it is not significant and will be less than the 0.1% of the dataset.

## Modelling (Optional)

No model has been made due to we focus more on data analysis

### Evaluation

No evaluation of the model was done 

### Deployment

No deployment more than the jupyter notebook file

## Statistics

We made an statistical information method and in the notebook you will see the increase or decrease of the crimes in times between a year, month or week basis in a pandas dataframe.