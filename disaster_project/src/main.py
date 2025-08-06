import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import csv
import os

conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Saikiran@23134',
    database='disaster'
)
cursor = conn.cursor()

def show_menu():
    print("\n--- Disaster Data Operations ---")
    print("1. Total number of disasters by type")
    print("2. Avg estimation cost by disaster type")
    print("3. Top affected locations")
    print("4. Average injuries per disaster")
    print("5. Disasters with missing impact data")
    print("6. Top 5 disasters by total deaths")
    print("7. Total damage by origin")
    print("8. Monthly disaster counts")
    print("9. Disasters with over 10,000 affected")
    print("10. Impact: Null vs Non-null count")
    print("Type 'q' to quit")

def export_query_to_csv(cursor, sql_query, csv_path, params=None):
    if params:
        cursor.execute(sql_query, params)
    else:
        cursor.execute(sql_query)

    results = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(results, columns=columns)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    df.to_csv(csv_path, index=True)

    try:
        os.startfile(csv_path)
    except Exception:
        print(f"Saved CSV to: {csv_path}")
    return df

while True:
    show_menu()
    choice = input("Enter choice number or 'q' to quit: ").strip()

    if choice.lower() == 'q':
        print("👋 Exiting. Thanks for exploring the data!")
        break

    try:
        num = int(choice)

        if num == 1:
            query=" SELECT disaster_type, COUNT(*) AS total FROM disaster_data GROUP BY disaster_type; "
            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()

            csv_filename = r"C:\Revature_individual\disaster_project\result\total_disasters.csv"
            df_disasters=export_query_to_csv(cursor, query, csv_filename)

            if show_graph == "yes":
                plt.figure(figsize=(10, 6))
                plt.bar(df_disasters['disaster_type'], df_disasters['total'], color='skyblue')
                plt.title('Total Number of Disasters by Type')
                plt.xlabel('Disaster Type')
                plt.ylabel('Count')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.show()

        elif num == 2:
            avg_costs = {
            1: ('Flood', 860),
            2: ('Flashflood', 1200),
            3: ('Storm', 1400),
            4: ('Glacial lake outburst flood', 8000),
            5: ('Earthquake', 2100)}

            print("Choose a disaster type:")
            print("1. Flood\n2. Flashflood\n3. Storm\n4. Glacial lake outburst flood\n5. Earthquake")

            choice = int(input("Enter the number corresponding to the disaster type: "))
            selected_disaster, avg_cost = avg_costs.get(choice)

            sql_query = " SELECT disaster_type, location, no_affected, no_affected * %s AS estimated_total_cost FROM disaster_data WHERE disaster_type = %s AND no_affected IS NOT NULL;"
            params = (avg_cost, selected_disaster)

            csv_filename = r"C:\Revature_individual\disaster_project\result\estimated_cost_by_disaster.csv"
            df_cost = export_query_to_csv(cursor, sql_query, csv_filename, params)

        elif num == 3:
            query=" SELECT location, SUM(total_affected) AS affected FROM disaster_data  WHERE total_affected IS NOT NULL GROUP BY location ORDER BY affected DESC LIMIT 5 "
            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            csv_filename = r"C:\Revature_individual\disaster_project\result\top_affected_locations.csv"
            df_disasters=export_query_to_csv(cursor, query, csv_filename)
            if show_graph == "yes":
                plt.figure(figsize=(10, 6))
                plt.bar(df_disasters['location'], df_disasters['affected'], color='skyblue')
                plt.xlabel('Location')
                plt.ylabel('Total People Affected')
                plt.title('Top 5 Affected Locations by Disaster')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.grid(axis='y', linestyle='--', alpha=0.7)
                plt.show()
            

        elif num == 4:
            query = " SELECT disaster_type, ROUND(AVG(no_injured), 2) AS avg_injuries FROM disaster_data WHERE no_injured IS NOT NULL GROUP BY disaster_type ORDER BY avg_injuries DESC; "
            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            csv_filename = r"C:\Revature_individual\disaster_project\result\avg_injuries_by_disaster.csv"
            df_avg_injuries = export_query_to_csv(cursor, query, csv_filename)
            if show_graph == "yes":
                plt.figure(figsize=(10, 6))
                plt.bar(df_avg_injuries['disaster_type'], df_avg_injuries['avg_injuries'], color='salmon')
                plt.xlabel('Disaster Type')
                plt.ylabel('Average Number of Injuries')
                plt.title('Average Injuries by Disaster Type')
                plt.xticks(rotation=45, ha='right')
                plt.grid(axis='y', linestyle='--', alpha=0.5)
                plt.tight_layout()
                plt.show()
        elif num == 5:
            query="SELECT MONTH(start_date) AS month, COUNT(*) AS disaster_count FROM disaster_data GROUP BY MONTH(start_date) ORDER BY month;"
            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            csv_filename = r"C:\Revature_individual\disaster_project\result\disasters_with_missing_impact.csv"
            df_missing_impact = export_query_to_csv(cursor, query, csv_filename)
            if show_graph == "yes":
                plt.bar(df_missing_impact['month'], df_missing_impact['disaster_count'])
                plt.xlabel('Month')
                plt.ylabel('Disaster Frequency')
                plt.title('Disaster Frequency by Month')
                plt.xticks(range(1, 13))
                plt.grid(True)
                plt.show()
        elif num == 6:
            query = " SELECT location, disaster_type, total_deaths  FROM disaster_data WHERE total_deaths IS NOT NULL ORDER BY total_deaths DESC LIMIT 5;"
            
            csv_filename = r"C:\Revature_individual\disaster_project\result\top_5_deadliest_disasters.csv"
            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            df_top_deaths = export_query_to_csv(cursor, query, csv_filename)
            if show_graph == "yes":
                labels = df_top_deaths.apply(lambda row: f"{row['location']} ({row['disaster_type']})", axis=1)
                
                plt.figure(figsize=(10, 6))
                plt.barh(labels, df_top_deaths['total_deaths'], color='firebrick')
                plt.xlabel('Total Deaths')
                plt.title('Top 5 Deadliest Disasters')
                plt.gca().invert_yaxis()
                plt.grid(axis='x', linestyle='--', alpha=0.5)
                plt.tight_layout()
                plt.show()
        elif num == 7:
            query = """
                SELECT origin, SUM(Total_Deaths) AS total_deaths 
                FROM disaster_data 
                WHERE Total_Deaths IS NOT NULL 
                GROUP BY origin 
                ORDER BY SUM(Total_Deaths) DESC;
            """

            csv_filename = r"C:\Revature_individual\disaster_project\result\damage_by_origin.csv"
            df_damage_by_origin = export_query_to_csv(cursor, query, csv_filename)
            df_damage_by_origin['origin'] = df_damage_by_origin['origin'].fillna('Unknown')

            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            if show_graph == "yes":
                plt.figure(figsize=(10, 6))
                plt.bar(df_damage_by_origin['origin'], df_damage_by_origin['total_deaths'], color='darkgreen')
                plt.xlabel('Disaster Origin')
                plt.ylabel('Total Deaths')
                plt.title('Total Deaths by Origin')
                plt.xticks(rotation=45, ha='right')
                plt.grid(True, linestyle='--', alpha=0.5)
                plt.tight_layout()
                plt.show()
        elif num == 8:
            query = """
                SELECT Disaster_Type, SUM(Total_Deaths) AS death_count
                FROM disaster_data
                GROUP BY Disaster_Type
                ORDER BY death_count DESC;
            """
            csv_filename = r"C:\Revature_individual\disaster_project\result\deaths_by_type.csv"

            df_plot = export_query_to_csv(cursor, query, csv_filename)

            plt.figure(figsize=(10, 6))
            plt.bar(df_plot['Disaster_Type'], df_plot['death_count'], color='salmon')
            plt.title('Deaths by Disaster Type')
            plt.xticks(rotation=45, ha='right')
            plt.xlabel('Disaster Type')
            plt.ylabel('Total Deaths')
            plt.grid(axis='y', linestyle='--', alpha=0.5)
            plt.tight_layout()
            plt.show()
        elif num == 9:
            query = """
                SELECT Disaster_Type, Location, Total_Affected
                FROM disaster_data
                WHERE Total_Affected > 10000
                ORDER BY Total_Affected DESC;
            """

            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            csv_filename = r"C:\Revature_individual\disaster_project\result\high_impact_disasters.csv"
            
            df_high_impact = export_query_to_csv(cursor, query, csv_filename)

            if show_graph == "yes":
                plt.figure(figsize=(12, 6))
                df_high_impact['label'] = df_high_impact['Disaster_Type'] + " @ " + df_high_impact['Location']
                
                plt.barh(df_high_impact['label'], df_high_impact['Total_Affected'], color='darkorange')
                plt.title('Disasters Affecting Over 10,000 People')
                plt.xlabel('Total Affected')
                plt.ylabel('Disaster Type & Location')
                plt.grid(axis='x', linestyle='--', alpha=0.5)
                plt.tight_layout()
                plt.show()
        elif num == 10:
            query = "SELECT COUNT(CASE WHEN no_affected IS NULL THEN 1 END) AS null_count, COUNT(CASE WHEN no_affected IS NOT NULL THEN 1 END) AS filled_count FROM disaster_data;"

            show_graph = input("Do you want to see a graphical view? (yes/no): ").strip().lower()
            csv_filename = r"C:\Revature_individual\disaster_project\result\null_vs_filled_no_affected.csv"
            
            df_nulls = export_query_to_csv(cursor, query, csv_filename)

            if show_graph == "yes":
                df_plot = df_nulls.T.reset_index()
                df_plot.columns = ['Category', 'Count']

                plt.figure(figsize=(8, 5))
                plt.bar(df_plot['Category'], df_plot['Count'], color=['tomato', 'seagreen'])
                plt.title("NULL vs Filled Count in 'no_affected'")
                plt.ylabel("Number of Records")
                plt.grid(axis='y', linestyle='--', alpha=0.5)
                plt.tight_layout()
                plt.show()
        else:
            print("❌ Invalid option. Choose from 1 to 10 or 'q' to quit.")
            continue
  
    except ValueError:
        print("⚠️ Please enter a number from 1 to 10, or 'q' to quit.")

cursor.close()
conn.close()