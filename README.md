# Project-1-Census-Data-Standardization-and-Analysis-Pipeline-From-Cleaning-to-Visualization

## Project Overview
This project involves cleaning, processing, and analyzing census data from a given source. The tasks include data renaming, handling missing data, standardizing state/UT names, managing new state/UT formations, storing data, connecting to databases, and querying data. The goal is to ensure uniformity, accuracy, and accessibility of the census data for further analysis and visualization.

## Tools Used

* Python: For data processing and analysis.
* MongoDB: For storing the processed data.
* Relational Database (e.g., MySQL): For querying the data.
* Streamlit: For displaying query results.
  
## Tasks Performed

### 1. Rename Column Names:

Rename columns for uniformity across datasets.

### 2. Rename State/UT Names:

Standardize the format of State/UT names to title case, with "and" in lowercase.

### 3. New State/UT Formation:

Handle the formation of Telangana from Andhra Pradesh and Ladakh from Jammu and Kashmir.

### 4. Find and Process Missing Data:

Identify missing data, fill in where possible, and compare the amount of missing data before and after the filling process.

### 5. Save Data to MongoDB:

Save the processed data to MongoDB in a collection named "census".

### 6. Database Connection and Data Upload:

Fetch data from MongoDB and upload it to a relational database, ensuring proper primary and foreign key constraints.

### 7. Run Queries on the Database:

Perform various queries to gather insights from the data.

## Usage

### 1. Clone the Repository:
`
git clone https://github.com/ybk08/Project-1-Census-Data-Standardization-and-Analysis-Pipeline-From-Cleaning-to-Visualization.git

cd Project-1-Census-Data-Standardization-and-Analysis-Pipeline-From-Cleaning-to-Visualization
`
### 2. Run the Script:

`python -m streamlit run census.py` : To show output using streamlit

### 3. Access the Data:

Data will be saved to MongoDB and can be uploaded to a relational database as per the code in census.py.

## File Structure

`census_2011.xlsx`: Contains the raw data.

`census.py`: Contains the complete code for the project, organized into sections for data pipeline and analysis.

`Telangana.txt`: Contains districts after the formation of Telangana from Andhra Pradesh. 

`db_credentials.txt`: Contains Database username and password.

## Contact Information

For questions or support, please contact yashkoli995@gmail.com.
