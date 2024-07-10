import pandas as pd
import pymongo
import mysql.connector
import matplotlib.pyplot as plt
import streamlit as st


# Task 1: Rename Column Names
def rename_columns(df):
    # Define a dictionary to map the current column names to the new column names
    column_mapping = {
        'District code': 'District_code',
        'State name': 'State/UT',
        'District name': 'District',
        'Male_Literate': 'Literate_Male',
        'Female_Literate': 'Literate_Female',
        'Rural_Households': 'Households_Rural',
        'Urban_Households': 'Households_Urban',
        'Age_Group_0_29': 'Young_and_Adult',
        'Age_Group_30_49': 'Middle_Aged',
        'Age_Group_50': 'Senior_Citizen',
        'Age not stated': 'Age_Not_Stated',
		'Households_with_TV_Computer_Laptop_Telephone_mobile_phone_and_Scooter_Car': 'Multi_Amenities_Households',
		'Type_of_latrine_facility_Night_soil_disposed_into_open_drain_Households': 'Latrine_Nightsoil_Open_Drain_Households',
		'Type_of_latrine_facility_Flush_pour_flush_latrine_connected_to_other_system_Households': 'Latrine_Flush_Connected_Other_System_Households',
		'Not_having_latrine_facility_within_the_premises_Alternative_source_Open_Households': 'No_Latrine_Open_Source_Households',
		'Main_source_of_drinking_water_Handpump_Tubewell_Borewell_Households': 'Drinking_Water_Handpump_Tubewell_Borewell_Households',
		'Main_source_of_drinking_water_Other_sources_Spring_River_Canal_Tank_Pond_Lake_Other_sources__Households': 'Drinking_Water_Other_Sources_Households'
    }

    # Rename the columns of the DataFrame using the mapping defined above
    df.rename(columns=column_mapping, inplace=True)

    # Return the modified DataFrame with the new column names
    return df


# Task 2: Rename State/UT Names
def rename_states(df):
    # Convert all State/UT names to title case
    df['State/UT'] = df['State/UT'].str.title()

    # Replace ' And ' with ' and ' to ensure consistent formatting in State/UT names
    df['State/UT'] = df['State/UT'].str.replace(' And ', ' and ',regex = False)

    # Return the modified DataFrame with renamed State/UT names
    return df


# Task 3: New State/UT Formation
def handle_new_states(df):
    # Load district data for Telangana from the file 'Telangana.txt'
    with open('Telangana.txt', 'r') as file:
        telangana_districts = file.read().splitlines()

    # Update the 'State/UT' column to 'Telangana' for districts listed in 'Telangana.txt'
    df.loc[df['District'].isin(telangana_districts), 'State/UT'] = 'Telangana'

    # Update the 'State/UT' column to 'Ladakh' for districts 'Leh(Ladakh)' and 'Kargil'
    df.loc[df['District'].isin(['Leh(Ladakh)', 'Kargil']), 'State/UT'] = 'Ladakh'

    # Return the modified DataFrame with the updated State/UT
    return df


# Task 4: Find and Process Missing Data
def handle_missing_data(df):
    # Calculate the percentage of missing data for specific columns before handling missing data
    missing_percentage_before = (df[['Literate', 'Female', 'Households', 'Male', 'Population']].isna().sum() / len(df)) * 100

    # Check if any value in the 'Population' column is missing (NaN)
    if df['Population'].isna().any():
        # Fill missing 'Population' values by summing 'Male' and 'Female' values
        df.loc[df['Population'].isna(), 'Population'] = df['Male'] + df['Female']

    # Check if any value in the 'Male' column is missing (NaN)
    if df['Male'].isna().any():
        # Fill missing 'Male' values by subtracting 'Female' from 'Population'
        df.loc[df['Male'].isna(), 'Male'] = df['Population'] - df['Female']

    # Check if any value in the 'Female' column is missing (NaN)
    if df['Female'].isna().any():
        # Fill missing 'Female' values by subtracting 'Male' from 'Population'
        df.loc[df['Female'].isna(), 'Female'] = df['Population'] - df['Male']

    # Check if any value in the 'Literate' column is missing (NaN)
    if df['Literate'].isna().any():
        # Fill missing 'Literate' values by summing 'Literate_Male' and 'Literate_Female' values
        df.loc[df['Literate'].isna(), 'Literate'] = df['Literate_Male'] + df['Literate_Female']

    # Check if any value in the 'Households' column is missing (NaN)
    if df['Households'].isna().any():
        # Fill missing 'Households' values by summing 'Households_Rural' and 'Households_Urban' values
        df.loc[df['Households'].isna(), 'Households'] = df['Households_Rural'] + df['Households_Urban']

    # Calculate the percentage of missing data for specific columns after handling missing data
    missing_percentage_after = (df[['Literate', 'Female', 'Households', 'Male', 'Population']].isna().sum() / len(df)) * 100
    
    # Create a DataFrame to compare missing data percentages before and after handling missing data
    compare = pd.DataFrame([missing_percentage_before, missing_percentage_after]).T
    compare.columns = ['Missing_data_before(%)','Missing_data_after(%)']

    # Plot the comparison of missing data percentages as a horizontal bar chart
    st.title('Comparison of missing data before and after the data-filling process was done!')
    
    # Create a Figure object
    fig, ax = plt.subplots(figsize=(10, 5))
    compare.plot(kind='barh', color=['brown', 'skyblue'], ax=ax)
    ax.set_xlabel('Percent(%)')

    # Display the plot in Streamlit
    st.pyplot(fig)    

    # Return the modified DataFrame with the handled missing data
    return df


# Task 5: Save Data to MongoDB
def save_to_mongodb(df):
    try:
        # Create a connection to the MongoDB server running on localhost at the default port 27017
        client = pymongo.MongoClient("mongodb://localhost:27017/")

        # Access the 'census_db' database. If it doesn't exist, MongoDB will create it.
        db = client["census_db"]

        # Access the 'census' collection within the 'census_db' database. If it doesn't exist, MongoDB will create it.
        collection = db["census"]

        # Convert the DataFrame to a list of dictionaries (records) and insert them into the 'census' collection.
        # Each dictionary corresponds to a document in the MongoDB collection.
        collection.insert_many(df.to_dict('records'))
    
    except Exception as e:
        # Handle exceptions that may occur during the connection or data insertion process
        st.error(f"An error occurred while saving the data to MangoDB: {e}")

    finally:
        # Close the connection to the MongoDB server
        client.close()


# Task 6: Database connection and data upload

# Function to fetch data from MongoDB
def fetch_from_mongodb():
    try:
        # Create a connection to the MongoDB server running on localhost at the default port 27017
        client = pymongo.MongoClient("mongodb://localhost:27017/")

        # Access the 'census_db' database.  
        db = client["census_db"]

        # Access the 'census' collection within the 'census_db' database.
        collection = db["census"]

        # Query all documents in the 'census' collection
        cursor = collection.find()

        # Convert the cursor (which contains all documents) to a list of dictionaries, and then create a DataFrame from this list 
        df = pd.DataFrame(list(cursor))
    
    except Exception as e:
        # Handle exceptions that may occur during the connection or data extraction process
        st.error(f"An error occurred while extracting the data from MangoDB: {e}")
    
    finally:
        # Close the connection to the MongoDB server
        client.close()

    # Return the DataFrame containing the data fetched from MongoDB
    return df


# Function to read database credentials
def read_db_credentials(filename):
    # Create an empty dict
    global credentials
    credentials = {}
    with open(filename, 'r') as file:
        for line in file:
            # Split each line by the colon to get key and value
            key, value = line.strip().split(':')
            # Strip any leading/trailing whitespace from key and value and add to the dictionary
            credentials[key.strip()] = value.strip()


# Function to create tables
def create_mysql_tables():
    try:
        # Establish the database connection
        db_connection = mysql.connector.connect(
            host="localhost",
            user=credentials['user'],
            password=credentials['password'],
            auth_plugin='mysql_native_password'
        )
        if db_connection.is_connected():
            cursor = db_connection.cursor()

            # Create the database
            cursor.execute("CREATE DATABASE IF NOT EXISTS census_db")
            
            # Use the created database
            cursor.execute("USE census_db")

            # SQL statements to create tables
            create_table_states = """
            CREATE TABLE IF NOT EXISTS States (
                state_id INT AUTO_INCREMENT PRIMARY KEY,
                State_or_UT VARCHAR(255) UNIQUE NOT NULL
            )
            """

            create_table_districts = """
            CREATE TABLE IF NOT EXISTS Districts (
                District_code INT PRIMARY KEY,
                District VARCHAR(255) UNIQUE NOT NULL,
                state_id INT,
                FOREIGN KEY (state_id) REFERENCES States(state_id)
            )
            """

            create_table_census_data = """
            CREATE TABLE IF NOT EXISTS Census_Data (
                census_id INT AUTO_INCREMENT PRIMARY KEY,
                District_code INT,
                Population BIGINT,
                male BIGINT,
                female BIGINT,
                literate BIGINT,
                Literate_Male BIGINT,
                Literate_Female BIGINT,
                sc BIGINT,
		        Male_SC BIGINT,
                Female_SC BIGINT,
                st BIGINT,
		        Male_ST BIGINT,
                Female_ST BIGINT,
                workers BIGINT,
                male_workers BIGINT,
                female_workers BIGINT,
                main_workers BIGINT,
                marginal_workers BIGINT,
		        Non_Workers BIGINT,
                Cultivator_Workers BIGINT,
                Agricultural_Workers BIGINT,
                household_workers BIGINT,
                other_workers BIGINT,
                hindus BIGINT,
                muslims BIGINT,
                christians BIGINT,
                sikhs BIGINT,
                buddhists BIGINT,
                jains BIGINT,
                others_religions BIGINT,
                religion_not_stated BIGINT,
                below_primary_education BIGINT,
                primary_education BIGINT,
                middle_education BIGINT,
                secondary_education BIGINT,
                higher_education BIGINT,
                graduate_education BIGINT,
                other_education BIGINT,
                literate_education BIGINT,
                illiterate_education BIGINT,
                total_education BIGINT,
                Young_and_Adult BIGINT,
                Middle_Aged BIGINT,
                Senior_Citizen BIGINT,
                Age_Not_Stated BIGINT,
                power_parity_less_than_rs_45000 BIGINT,
                power_parity_rs_45000_90000 BIGINT,
                power_parity_rs_90000_150000 BIGINT,
                power_parity_rs_45000_150000 BIGINT,
                power_parity_rs_150000_240000 BIGINT,
                power_parity_rs_240000_330000 BIGINT,
                power_parity_rs_150000_330000 BIGINT,
                power_parity_rs_330000_425000 BIGINT,
                power_parity_rs_425000_545000 BIGINT,
                power_parity_rs_330000_545000 BIGINT,
                power_parity_above_rs_545000 BIGINT,
                total_power_parity BIGINT,
                FOREIGN KEY (District_code) REFERENCES Districts(District_code)
            )
            """

            create_table_household_data = """
            CREATE TABLE IF NOT EXISTS Household_Data (
                household_id INT AUTO_INCREMENT PRIMARY KEY,
                District_code INT,
		        LPG_or_PNG_Households BIGINT,
                Housholds_with_Electric_Lighting BIGINT,
                Households_with_Internet BIGINT,
                Households_with_Computer BIGINT,
                Households_Rural BIGINT,
                Households_Urban BIGINT,
                households BIGINT,
                households_with_bicycle BIGINT,
                households_with_car_jeep_van BIGINT,
                households_with_radio_transistor BIGINT,
                households_with_scooter_motorcycle_moped BIGINT,
                households_with_telephone_mobile_phone_landline_only BIGINT,
                households_with_telephone_mobile_phone_mobile_only BIGINT,
                multi_amenities_households BIGINT,
                households_with_television BIGINT,
                households_with_telephone_mobile_phone BIGINT,
                households_with_telephone_mobile_phone_both BIGINT,
                condition_of_occupied_census_houses_dilapidated_households BIGINT,
                households_with_separate_kitchen_cooking_inside_house BIGINT,
                having_bathing_facility_total_households BIGINT,
                having_latrine_facility_within_the_premises_total_households BIGINT,
                ownership_owned_households BIGINT,
                ownership_rented_households BIGINT,
                type_of_bathing_facility_enclosure_without_roof_households BIGINT,
                type_of_fuel_used_for_cooking_any_other_households BIGINT,
                type_of_latrine_facility_pit_latrine_households BIGINT,
                type_of_latrine_facility_other_latrine_households BIGINT,
                latrine_nightsoil_open_drain_households BIGINT,
                latrine_flush_connected_other_system_households BIGINT,
                not_having_bathing_facility_within_the_premises_total_households BIGINT,
                no_latrine_open_source_households BIGINT,
                main_source_of_drinking_water_un_covered_well_households BIGINT,
                drinking_water_handpump_tubewell_borewell_households BIGINT,
                main_source_of_drinking_water_spring_households BIGINT,
                main_source_of_drinking_water_river_canal_households BIGINT,
                main_source_of_drinking_water_other_sources_households BIGINT,
                drinking_water_other_sources_households BIGINT,
                location_of_drinking_water_source_near_the_premises_households BIGINT,
                location_of_drinking_water_source_within_the_premises_households BIGINT,
                main_source_of_drinking_water_tank_pond_lake_households BIGINT,
                main_source_of_drinking_water_tapwater_households BIGINT,
                main_source_of_drinking_water_tubewell_borehole_households BIGINT,
                household_size_1_person_households BIGINT,
                household_size_2_persons_households BIGINT,
                household_size_1_to_2_persons BIGINT,
                household_size_3_persons_households BIGINT,
                household_size_3_to_5_persons_households BIGINT,
                household_size_4_persons_households BIGINT,
                household_size_5_persons_households BIGINT,
                household_size_6_8_persons_households BIGINT,
                household_size_9_persons_and_above_households BIGINT,
                location_of_drinking_water_source_away_households BIGINT,
                married_couples_1_households BIGINT,
                married_couples_2_households BIGINT,
                married_couples_3_households BIGINT,
                married_couples_3_or_more_households BIGINT,
                married_couples_4_households BIGINT,
                married_couples_5_households BIGINT,
                married_couples_none_households BIGINT,
                FOREIGN KEY (District_code) REFERENCES Districts(District_code)
            )
            """

            # Execute SQL statements to create tables
            cursor.execute(create_table_states)
            cursor.execute(create_table_districts)
            cursor.execute(create_table_census_data)
            cursor.execute(create_table_household_data)

            # Commit the changes to the database
            db_connection.commit()
            
            # Close the cursor
            cursor.close()
        
        else:
            print("Failed to connect to the database.")
    
    except mysql.connector.Error as e:
        st.error(f"An error occurred while creating tables: {e}")

    finally:
        # Close the database connection
        if db_connection.is_connected():
            db_connection.close()

# Function to upload States data
def upload_to_states_table(df):
    try:
        # Connect to MySQL
        db_connection = mysql.connector.connect(
            host="localhost",
            user=credentials['user'],
            password=credentials['password'],
            database="census_db"
        )
        cursor = db_connection.cursor()

        # Insert unique State records
        for _, row in df.iterrows():
            # Check if the state already exists
            cursor.execute("SELECT state_id FROM States WHERE State_or_UT = %s", (row['State/UT'],))
            result = cursor.fetchone()
            
            # If the state does not exist, insert it
            if result is None:
                sql = "INSERT INTO States (State_or_UT) VALUES (%s)"
                cursor.execute(sql, (row['State/UT'],))

        # Commit the changes to the database
        db_connection.commit()

        # Close the cursor
        cursor.close()

    except mysql.connector.Error as e:
        st.error(f"An error occurred while uploading data to states table: {e}")

    finally:
        # Close the database connection
        if db_connection.is_connected():
            db_connection.close()


# Function to upload districts data
def upload_to_districts_table(df):
    try:
        # Connect to MySQL
        db_connection = mysql.connector.connect(
            host="localhost",
            user=credentials['user'],
            password=credentials['password'],
            database="census_db"
        )
        cursor = db_connection.cursor()

        # Insert unique District records
        for _, row in df.iterrows():
            try:
                # Retrieve state_id for the given state_name
                cursor.execute("SELECT state_id FROM States WHERE State_or_UT = %s", (row['State/UT'],))
                state_result = cursor.fetchone()

                if state_result is not None:
                    state_id = state_result[0]
                
                    # Check if the district already exists
                    cursor.execute("SELECT District_code FROM Districts WHERE  District = %s", (row['District'],))
                    result = cursor.fetchone()
                
                    # If the district does not exist, insert it
                    if result is None:
                        sql = "INSERT INTO Districts (District_code, District, state_id) VALUES (%s, %s, %s)"
                        cursor.execute(sql, (row['District_code'], row['District'], state_id))

            except mysql.connector.Error as e:
                st.error(f"Error processing districts row {row}: {e}")

        # Commit the changes to the database
        db_connection.commit()

        # Close the cursor
        cursor.close()

    except mysql.connector.Error as e:
        st.error(f"An error occurred while uploading data to districts table: {e}")

    finally:
        # Close the database connection
        if db_connection.is_connected():
            db_connection.close()
            

# Function to upload census data
def upload_to_census_data_table(df):
    try:
        # Connect to MySQL
        db_connection = mysql.connector.connect(
            host="localhost",
            user=credentials['user'],
            password=credentials['password'],
            database="census_db"
        )
        cursor = db_connection.cursor()

        # Replace NaN values with 0
        df = df.fillna(value=0)

        # Insert CensusData records
        for _, row in df.iterrows():
            try:
                # Retrieve District_code for the given District
                cursor.execute("SELECT District_code FROM Districts WHERE District = %s", (row['District'],))
                district_result = cursor.fetchone()

                if district_result is not None:
                    District_code = district_result[0]
                
                    # Prepare SQL statement
                    sql = """
                        INSERT INTO Census_Data (District_code, Population, male, female, literate, Literate_Male, 
                        Literate_Female, sc, Male_SC, Female_SC, st, Male_ST, Female_ST, workers, male_workers, female_workers, 
                        main_workers, marginal_workers, Non_Workers, Cultivator_Workers, Agricultural_Workers, household_workers, other_workers, hindus, muslims, 
                        christians, sikhs, buddhists, jains, others_religions, religion_not_stated, below_primary_education, primary_education, middle_education, secondary_education, 
                        higher_education, graduate_education, other_education, literate_education, illiterate_education, total_education, Young_and_Adult, Middle_Aged, Senior_Citizen, 
                        Age_Not_Stated, power_parity_less_than_rs_45000, power_parity_rs_45000_90000, power_parity_rs_90000_150000, power_parity_rs_45000_150000, power_parity_rs_150000_240000, 
                        power_parity_rs_240000_330000, power_parity_rs_150000_330000, power_parity_rs_330000_425000, power_parity_rs_425000_545000, power_parity_rs_330000_545000, 
                        power_parity_above_rs_545000, total_power_parity)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    # Execute SQL statement with row data
                    cursor.execute(sql, (
                        District_code, row['Population'], row['Male'], row['Female'], row['Literate'], row['Literate_Male'], 
                        row['Literate_Female'], row['SC'], row['Male_SC'], row['Female_SC'], row['ST'], row['Male_ST'], row['Female_ST'], row['Workers'], row['Male_Workers'], row['Female_Workers'], 
                        row['Main_Workers'], row['Marginal_Workers'], row['Non_Workers'], row['Cultivator_Workers'], row['Agricultural_Workers'], row['Household_Workers'], row['Other_Workers'], 
                        row['Hindus'], row['Muslims'], row['Christians'], row['Sikhs'], row['Buddhists'], row['Jains'], 
                        row['Others_Religions'], row['Religion_Not_Stated'], row['Below_Primary_Education'], row['Primary_Education'], 
                        row['Middle_Education'], row['Secondary_Education'], row['Higher_Education'], row['Graduate_Education'], 
                        row['Other_Education'], row['Literate_Education'], row['Illiterate_Education'], row['Total_Education'], 
                        row['Young_and_Adult'], row['Middle_Aged'], row['Senior_Citizen'], row['Age_Not_Stated'], 
                        row['Power_Parity_Less_than_Rs_45000'], row['Power_Parity_Rs_45000_90000'], row['Power_Parity_Rs_90000_150000'], 
                        row['Power_Parity_Rs_45000_150000'], row['Power_Parity_Rs_150000_240000'], row['Power_Parity_Rs_240000_330000'], 
                        row['Power_Parity_Rs_150000_330000'], row['Power_Parity_Rs_330000_425000'], row['Power_Parity_Rs_425000_545000'], 
                        row['Power_Parity_Rs_330000_545000'], row['Power_Parity_Above_Rs_545000'], row['Total_Power_Parity']
                    ))
                    
            except mysql.connector.Error as e:
                print(f"Error processing CensusData row {row}: {e}")

        # Commit the changes to the database
        db_connection.commit()

        # Close the cursor
        cursor.close()

    except mysql.connector.Error as e:
        print(f"An error occurred while uploading data to census_data table: {e}")

    finally:
        # Close the database connection
        if db_connection.is_connected():
            db_connection.close()


# Function to upload household data
def upload_to_household_data_table(df):
    try:
        # Connect to MySQL
        db_connection = mysql.connector.connect(
            host="localhost",
            user=credentials['user'],
            password=credentials['password'],
            database="census_db"
        )
        cursor = db_connection.cursor()

        # Replace NaN values with 0
        df = df.fillna(value=0)

        # Insert HouseholdData records
        for _, row in df.iterrows():
            try:
                # Retrieve District_code for the given District
                cursor.execute("SELECT District_code FROM Districts WHERE District = %s", (row['District'],))
                district_result = cursor.fetchone()

                if district_result is not None:
                    District_code = district_result[0]

                    # Prepare SQL statement
                    sql = """
                        INSERT INTO Household_Data (District_code, LPG_or_PNG_Households, Housholds_with_Electric_Lighting, Households_with_Internet, Households_with_Computer, Households_Rural, 
                        Households_Urban, households, households_with_bicycle, households_with_car_jeep_van, households_with_radio_transistor, households_with_scooter_motorcycle_moped, 
                        households_with_telephone_mobile_phone_landline_only, households_with_telephone_mobile_phone_mobile_only, multi_amenities_households, households_with_television, 
                        households_with_telephone_mobile_phone, households_with_telephone_mobile_phone_both, condition_of_occupied_census_houses_dilapidated_households, households_with_separate_kitchen_cooking_inside_house, 
                        having_bathing_facility_total_households, having_latrine_facility_within_the_premises_total_households, ownership_owned_households, ownership_rented_households, type_of_bathing_facility_enclosure_without_roof_households, 
                        type_of_fuel_used_for_cooking_any_other_households, type_of_latrine_facility_pit_latrine_households, type_of_latrine_facility_other_latrine_households, latrine_nightsoil_open_drain_households, 
                        latrine_flush_connected_other_system_households, not_having_bathing_facility_within_the_premises_total_households, no_latrine_open_source_households, main_source_of_drinking_water_un_covered_well_households, 
                        drinking_water_handpump_tubewell_borewell_households, main_source_of_drinking_water_spring_households, main_source_of_drinking_water_river_canal_households, main_source_of_drinking_water_other_sources_households, 
                        drinking_water_other_sources_households, location_of_drinking_water_source_near_the_premises_households, location_of_drinking_water_source_within_the_premises_households, main_source_of_drinking_water_tank_pond_lake_households, 
                        main_source_of_drinking_water_tapwater_households, main_source_of_drinking_water_tubewell_borehole_households, household_size_1_person_households, household_size_2_persons_households, household_size_1_to_2_persons, 
                        household_size_3_persons_households, household_size_3_to_5_persons_households, household_size_4_persons_households, household_size_5_persons_households, household_size_6_8_persons_households, 
                        household_size_9_persons_and_above_households, location_of_drinking_water_source_away_households, married_couples_1_households, married_couples_2_households, married_couples_3_households, married_couples_3_or_more_households, 
                        married_couples_4_households, married_couples_5_households, married_couples_none_households)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """
                    # Execute SQL statement with row data
                    cursor.execute(sql,( 
                        District_code, row['LPG_or_PNG_Households'], row['Housholds_with_Electric_Lighting'], row['Households_with_Internet'], row['Households_with_Computer'], row['Households_Rural'],  
                        row['Households_Urban'], row['Households'], row['Households_with_Bicycle'], row['Households_with_Car_Jeep_Van'], row['Households_with_Radio_Transistor'], row['Households_with_Scooter_Motorcycle_Moped'],  
                        row['Households_with_Telephone_Mobile_Phone_Landline_only'], row['Households_with_Telephone_Mobile_Phone_Mobile_only'], row['Multi_Amenities_Households'], row['Households_with_Television'], row['Households_with_Telephone_Mobile_Phone'],  
                        row['Households_with_Telephone_Mobile_Phone_Both'], row['Condition_of_occupied_census_houses_Dilapidated_Households'], row['Households_with_separate_kitchen_Cooking_inside_house'], row['Having_bathing_facility_Total_Households'], row['Having_latrine_facility_within_the_premises_Total_Households'],  
                        row['Ownership_Owned_Households'], row['Ownership_Rented_Households'], row['Type_of_bathing_facility_Enclosure_without_roof_Households'], row['Type_of_fuel_used_for_cooking_Any_other_Households'], row['Type_of_latrine_facility_Pit_latrine_Households'],  
                        row['Type_of_latrine_facility_Other_latrine_Households'], row['Latrine_Nightsoil_Open_Drain_Households'], row['Latrine_Flush_Connected_Other_System_Households'], row['Not_having_bathing_facility_within_the_premises_Total_Households'], row['No_Latrine_Open_Source_Households'], 
                        row['Main_source_of_drinking_water_Un_covered_well_Households'], row['Drinking_Water_Handpump_Tubewell_Borewell_Households'], row['Main_source_of_drinking_water_Spring_Households'], row['Main_source_of_drinking_water_River_Canal_Households'], row['Main_source_of_drinking_water_Other_sources_Households'], 
                        row['Drinking_Water_Other_Sources_Households'], row['Location_of_drinking_water_source_Near_the_premises_Households'], row['Location_of_drinking_water_source_Within_the_premises_Households'], row['Main_source_of_drinking_water_Tank_Pond_Lake_Households'], 
                        row['Main_source_of_drinking_water_Tapwater_Households'], row['Main_source_of_drinking_water_Tubewell_Borehole_Households'], row['Household_size_1_person_Households'], row['Household_size_2_persons_Households'], row['Household_size_1_to_2_persons'], 
                        row['Household_size_3_persons_Households'], row['Household_size_3_to_5_persons_Households'], row['Household_size_4_persons_Households'], row['Household_size_5_persons_Households'], row['Household_size_6_8_persons_Households'], 
                        row['Household_size_9_persons_and_above_Households'], row['Location_of_drinking_water_source_Away_Households'], row['Married_couples_1_Households'], row['Married_couples_2_Households'], row['Married_couples_3_Households'], 
                        row['Married_couples_3_or_more_Households'], row['Married_couples_4_Households'], row['Married_couples_5__Households'], row['Married_couples_None_Households']
                    ))

            except mysql.connector.Error as e:
                st.error(f"Error processing household_data row {row}: {e}")

        # Commit the changes to the database
        db_connection.commit()

        # Close the cursor
        cursor.close()

    except mysql.connector.Error as e:
        st.error(f"An error occurred while uploading data to household_data table: {e}")

    finally:
        # Close the database connection
        if db_connection.is_connected():
            db_connection.close()


# Task 7: Run Query on the database and show output on streamlit
def execute_query(query):
    # Connect to MySQL
    db_connection = mysql.connector.connect(
        host="localhost",
        user=credentials['user'],
        password=credentials['password'],
        database="census_db"
        )    
    cursor = db_connection.cursor(dictionary=True)
    
    cursor.execute(query)
    
    result = cursor.fetchall()
    
    cursor.close()
    
    db_connection.close()
    # Convert the result to a pandas DataFrame and return it
    return pd.DataFrame(result)


def get_total_population():
    query = """SELECT district, SUM(population) as total_population FROM census_data 
    JOIN districts ON census_data.District_code = districts.District_code GROUP BY district;"""
    return execute_query(query)


def get_literate_males_females():
    query = """
    SELECT district, SUM(Literate_Male) as literate_males, SUM(Literate_Female) as literate_females FROM census_data 
    JOIN districts ON census_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_worker_percentage():
    query = """
    SELECT district, 
    (SUM(male_workers) + SUM(female_workers)) / SUM(population) * 100 as worker_percentage
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_households_with_lpg_png():
    query = """
    SELECT district, SUM(LPG_or_PNG_Households) as households_with_lpg_png FROM household_data 
    JOIN districts ON household_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_religious_composition():
    query = """
    SELECT district, SUM(hindus) as hindus, SUM(muslims) as muslims, SUM(christians) as christians, 
    SUM(sikhs) as sikhs, SUM(buddhists) as buddhists, SUM(jains) as jains, SUM(others_religions) as other_religions, SUM(religion_not_stated) as religion_not_stated 
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_households_with_internet():
    query = """
    SELECT district, SUM(households_with_internet) as households_with_internet 
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_educational_attainment_distribution():
    query = """
    SELECT district, SUM(below_primary_education) as below_primary_education, SUM(primary_education) as primary_education, 
    SUM(middle_education) as middle_education, SUM(secondary_education) as secondary_education, SUM(higher_education) as higher_education, 
    SUM(graduate_education) as graduate_education, SUM(other_education) as other_education, SUM(literate_education) as literate_education,
    SUM(illiterate_education) as illiterate_education, SUM(total_education) as total_education
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_households_with_transportation_modes():
    query = """
    SELECT district, SUM(households_with_bicycle) as bicycle, SUM(households_with_car_jeep_van) as car, SUM(households_with_radio_transistor) as radio, 
    SUM(households_with_television) as television, SUM(households_with_scooter_motorcycle_moped) as bike
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_condition_of_census_houses():
    query = """
    SELECT district, SUM(condition_of_occupied_census_houses_dilapidated_households) as dilapidated, SUM(households_with_separate_kitchen_cooking_inside_house) as separate_kitchen, 
    SUM(having_bathing_facility_total_households) as bathing_facility, SUM(having_latrine_facility_within_the_premises_total_households) as latrine_facility
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_household_size_distribution():
    query = """
    SELECT district, SUM(household_size_1_person_households) as size_1_person, SUM(household_size_2_persons_households) as size_2_persons, 
    SUM(household_size_3_to_5_persons_households) as size_3_5_persons, SUM(household_size_6_8_persons_households) as size_6_8_persons, 
    SUM(household_size_9_persons_and_above_households) as size_9_persons_and_above
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code GROUP BY district;
    """
    return execute_query(query)


def get_total_households_in_each_state():
    query = """
    SELECT State_or_UT, SUM(households) as total_households FROM household_data 
    JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_households_with_latrine_facility_in_state():
    query = """
    SELECT State_or_UT, SUM(having_latrine_facility_within_the_premises_total_households) as households_with_latrine_facility
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_average_household_size_in_state():
    query = """
    SELECT State_or_UT, 
    AVG(household_size_2_persons_households) as size_2_persons_households,
    AVG(household_size_1_to_2_persons) as size_1_to_2_persons_households,
    AVG(household_size_3_persons_households) as size_3_persons_households,
    AVG(household_size_3_to_5_persons_households) as size_3_to_5_persons_households,
    AVG(household_size_4_persons_households) as size_4_persons_households,
    AVG(household_size_5_persons_households) as size_5_persons_households,
    AVG(household_size_6_8_persons_households) as size_6_8_persons_households,
    AVG(household_size_9_persons_and_above_households) as size_9_persons_and_above_households
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_households_owned_vs_rented_in_state():
    query = """
    SELECT State_or_UT, SUM(ownership_owned_households) as owned_households, SUM(ownership_rented_households) as rented_households
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_types_of_latrine_facilities_in_state():
    query = """
    SELECT State_or_UT, 
    SUM(type_of_latrine_facility_pit_latrine_households) as pit_latrine, SUM(latrine_flush_connected_other_system_households) as flush_latrine, 
    SUM(type_of_latrine_facility_other_latrine_households) as other_latrine, SUM(latrine_nightsoil_open_drain_households) as nightsoil_latrine,
    SUM(no_latrine_open_source_households) as no_latrine
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_households_with_nearby_drinking_water():
    query = """
    SELECT State_or_UT, SUM(drinking_water_handpump_tubewell_borewell_households) as households_with_nearby_drinking_water
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_average_household_income_distribution():
    query = """
    SELECT State_or_UT,
    AVG(power_parity_less_than_rs_45000) AS avg_less_than_rs_45000,
    AVG(power_parity_rs_45000_90000) AS avg_rs_45000_90000,
    AVG(power_parity_rs_90000_150000) AS avg_rs_90000_150000,
    AVG(power_parity_rs_45000_150000) AS avg_rs_45000_150000,
    AVG(power_parity_rs_150000_240000) AS avg_rs_150000_240000,
    AVG(power_parity_rs_240000_330000) AS avg_rs_240000_330000,
    AVG(power_parity_rs_150000_330000) AS avg_rs_150000_330000,
    AVG(power_parity_rs_330000_425000) AS avg_rs_330000_425000,
    AVG(power_parity_rs_425000_545000) AS avg_rs_425000_545000,
    AVG(power_parity_rs_330000_545000) AS avg_rs_330000_545000,
    AVG(power_parity_above_rs_545000) AS avg_above_rs_545000,
    AVG(total_power_parity) AS avg_total_power_parity
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_percentage_of_married_couples_with_household_size():
    query = """
    SELECT State_or_UT, (SUM(married_couples_1_households + married_couples_2_households + married_couples_3_households + married_couples_3_or_more_households 
    + married_couples_4_households + married_couples_5_households) / SUM(households)) * 100 as percentage_married_couples
    FROM household_data JOIN districts ON household_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_households_below_poverty_line():
    query = """
    SELECT State_or_UT, SUM(power_parity_less_than_rs_45000) AS households_below_poverty_line
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


def get_overall_literacy_rate():
    query = """
    SELECT State_or_UT, 
    (SUM(literate_education) / SUM(population)) * 100 as literacy_rate
    FROM census_data JOIN districts ON census_data.District_code = districts.District_code
    JOIN states ON states.state_id = districts.state_id GROUP BY State_or_UT;
    """
    return execute_query(query)


# Streamlit Display
def display_dataframes():
    st.title('Census Data Analysis')
    
    st.subheader('Total Population of Each District')
    st.dataframe(get_total_population())
    
    st.subheader('Literate Males and Females in Each District')
    st.dataframe(get_literate_males_females())
    
    st.subheader('Worker Percentage in Each District')
    st.dataframe(get_worker_percentage())
    
    st.subheader('Households with LPG or PNG as Cooking Fuel in Each District')
    st.dataframe(get_households_with_lpg_png())

    st.subheader('Religious Composition of Each District')
    st.dataframe(get_religious_composition())

    st.subheader('Households with Internet Access in Each District')
    st.dataframe(get_households_with_internet())

    st.subheader('Educational Attainment Distribution in Each District')
    st.dataframe(get_educational_attainment_distribution())
    
    st.subheader('Households with Access to Various Modes of Transportation in Each District')
    st.dataframe(get_households_with_transportation_modes())

    st.subheader('Condition of Occupied Census Houses in Each District')
    st.dataframe(get_condition_of_census_houses())

    st.subheader('Household Size Distribution in Each District')
    st.dataframe(get_household_size_distribution())

    st.subheader('Total Number of Households in Each State')
    st.dataframe(get_total_households_in_each_state())
    
    st.subheader('Households with Latrine Facility within the Premises in Each State')
    st.dataframe(get_households_with_latrine_facility_in_state())

    st.subheader('Average Household Size in Each State')
    st.dataframe(get_average_household_size_in_state())

    st.subheader('Households Owned vs Rented in Each State')
    st.dataframe(get_households_owned_vs_rented_in_state())

    st.subheader('Types of Latrine Facilities in Each State')
    st.dataframe(get_types_of_latrine_facilities_in_state())
    
    st.subheader('Households with Drinking Water Sources Near the Premises in Each State')
    st.dataframe(get_households_with_nearby_drinking_water())

    st.subheader('Average Household Income Distribution in Each State')
    st.dataframe(get_average_household_income_distribution())

    st.subheader('Percentage of Married Couples with Different Household Sizes in Each State')
    st.dataframe(get_percentage_of_married_couples_with_household_size())

    st.subheader('Households Below Poverty Line in Each State')
    st.dataframe(get_households_below_poverty_line())

    st.subheader('Overall Literacy Rate in Each State')
    st.dataframe(get_overall_literacy_rate())

def load_census_data(file_path):
    df = pd.read_excel(file_path)
    return df

if __name__ == '__main__':
    df = load_census_data('census_2011.xlsx')
    df = rename_columns(df)
    df = rename_states(df)
    df = handle_new_states(df)
    df = handle_missing_data(df)
    save_to_mongodb(df)
    fetch_from_mongodb()
    read_db_credentials('db_credentials.txt')    
    create_mysql_tables()
    upload_to_states_table(df)
    upload_to_districts_table(df)
    upload_to_census_data_table(df)
    upload_to_household_data_table(df)
    display_dataframes()


# to run - python -m streamlit run census.py