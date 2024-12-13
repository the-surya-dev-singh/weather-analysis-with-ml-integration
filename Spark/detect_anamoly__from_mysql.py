import time
import pandas as pd
import mysql.connector
from sklearn.preprocessing import StandardScaler
import pickle

#Load the trained model and scaler
with open('anomaly_scaler.pkl', 'rb') as scaler_file:
    scaler = pickle.load(scaler_file)

with open('anomaly_model.pkl', 'rb') as model_file:
    model = pickle.load(model_file)

#Define a function to connect to the MySQL database
def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        port="3306",
        password="",
        database="weather",
        autocommit=True
    )

#Fetch new data from the database
def fetch_new_data(last_id, conn):
    query = f"SELECT SQL_NO_CACHE * FROM weather_data WHERE `id` > '{last_id}' ORDER BY `id` ASC"
    new_data = pd.read_sql(query, conn)
    return new_data

#Preprocess and detect anomalies
def detect_anomalies(data):
    if data.empty:
        return data
    
    # Extract features for anomaly detection
    features = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)','Pressure (millibars)']
    data_scaled = scaler.transform(data[features])  # Normalize features
    
    # Predict anomalies
    data['Anomaly'] = model.predict(data_scaled)
    data['Anomaly'] = data['Anomaly'].apply(lambda x: 'Normal' if x == 1 else 'Anomalous')
    return data

#Main loop for real-time anomaly detection
def main():
    last_id = 0  # Keep track of the last processed date

    try:
        while True:
            # Fetch new data
            with get_db_connection() as conn:
                new_data = fetch_new_data(last_id, conn)
            
            if not new_data.empty:
                print("New data received:")
                print(new_data)
                
                # Detect anomalies
                processed_data = detect_anomalies(new_data)
                
                # Filter anomalies for reporting
                anomalies = processed_data[processed_data['Anomaly'] == 'Anomalous']
                
                if not anomalies.empty:
                    print("Anomalies detected:")
                    print(anomalies)
                else:
                    print("no anamoly detected")
                # Update the last processed ID
                last_id = new_data['id'].max()
            else:
                print("no new data")
            time.sleep(3)  # Wait for 3 seconds before fetching data again
    except KeyboardInterrupt:
        print("Real-time anomaly detection stopped.")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
