import pandas as pd
import mysql.connector
import re
import os

base_dir = os.path.dirname(__file__)
file_path = os.path.join(base_dir, '..', 'data', 'location_states_only.xlsx') 

# 1. Load Excel and normalize column names
df = pd.read_excel(file_path)

# Normalize column names: strip, lowercase, replace spaces and remove punctuation
df.columns = [
    re.sub(r'[^\w\s]', '', col).strip().replace(" ", "_").lower()
    for col in df.columns
]


# 2. Clean data: convert NaN → None
df = df.astype(object).where(pd.notnull(df), None)

# 3. Convert date column if present
if 'start_date' in df.columns:
    df['start_date'] = pd.to_datetime(
        df['start_date'], errors='coerce'
    ).dt.date

# 4. Prepare DB connection
conn = mysql.connector.connect(
    host='localhost',
    port=3306,
    user='root',
    password='Saikiran@23134',
    database='disaster'
)
cursor = conn.cursor()

cursor.execute(""" CREATE TABLE if not exists disaster_data (
    Classification_Key VARCHAR(100),
    Disaster_Subgroup VARCHAR(100),
    Disaster_Type VARCHAR(100),
    Disaster_Subtype VARCHAR(100),
    Location TEXT,
    Origin VARCHAR(100),
    Total_Deaths INT,
    No_Injured INT,
    No_Affected INT,
    No_Homeless INT,
    Total_Affected INT,
    Total_Damage DECIMAL(15,2),
    Total_Damage_Adjusted DECIMAL(15,2),
    Start_Date DATE,
    Duration_days INT
); """)

# 6. Insert rows with error handling
try:
    for _, row in df.iterrows():
        # Map DataFrame row → tuple in DB column order
        values = (
            row.get('classification_key'), row.get('disaster_subgroup'), row.get('disaster_type'),
            row.get('disaster_subtype'), row.get('location'), row.get('origin'),
            row.get('total_deaths'), row.get('no_injured'), row.get('no_affected'), row.get('no_homeless'),
            row.get('total_affected'), row.get('total_damage'), row.get('total_damage_adjusted'),
            row.get('start_date'), row.get('duration_days')
        )
        cursor.execute("""
            INSERT INTO disaster_data (
                classification_key, disaster_subgroup, disaster_type,
                disaster_subtype, location, origin,
                total_deaths, no_injured, no_affected, no_homeless,
                total_affected, total_damage, total_damage_adjusted,
                start_date, duration_days
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """, values)
    conn.commit()
    cursor.execute(""" delete from disaster_data where location is Null or location='' """)
    conn.commit()
    cursor.execute(""" ALTER TABLE disaster_data ADD COLUMN id INT NOT NULL PRIMARY KEY AUTO_INCREMENT FIRST """)
    conn.commit()

    print("✅ All data inserted successfully.")
except mysql.connector.Error as err:
    print("❌ Database error:", err)
finally:
    cursor.close()
    conn.close()