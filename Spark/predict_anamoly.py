import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import pickle

# Load dataset
data = pd.read_csv("weatherHistory.csv")  # Replace with your file path

#Preprocess the data
data.fillna(method='ffill', inplace=True)  # Fill missing values

# Features to use for anomaly detection
features = ['Temperature (C)', 'Humidity', 'Wind Speed (km/h)', 'Wind Bearing (degrees)','Pressure (millibars)']

# Normalize the features
scaler = StandardScaler()
data_scaled = scaler.fit_transform(data[features])

#Train an Isolation Forest model
model = IsolationForest(n_estimators=100, contamination=0.01, random_state=42)
model.fit(data_scaled)

# Save the scaler and model for real-time use
with open('anomaly_scaler.pkl', 'wb') as scaler_file:
    pickle.dump(scaler, scaler_file)

with open('anomaly_model.pkl', 'wb') as model_file:
    pickle.dump(model, model_file)

print("Anomaly detection model trained and saved.")

#Test anomaly detection on the same data
data['Anomaly'] = model.predict(data_scaled)
data['Anomaly'] = data['Anomaly'].apply(lambda x: 'Normal' if x == 1 else 'Anomalous')

# Output anomalies
anomalies = data[data['Anomaly'] == 'Anomalous']
print("Anomalies detected:")
print(anomalies)
