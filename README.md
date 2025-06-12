#  PhonePe Pulse Visualization Dashboard

A fully interactive and insightful **data visualization dashboard** built using **Streamlit**, **Plotly**, and **MySQL**. This project dynamically explores the **PhonePe Pulse Dataset**, providing choropleth maps, transaction and user analytics, and device brand trends at **state and district levels** across India.

### 🔗 [ Demo Video on LinkedIn](https://www.linkedin.com/posts/muniyappan-singaravel-23b277314_python-streamlit-plotly-activity-7338774688854163456-505d?utm_source=share&utm_medium=member_desktop&rcm=ACoAAE_F_48B4HBUJy1-Xx92qZDp-05xUsccZ-E)

---

##  Features

- **Interactive Choropleth Maps**
  - Explore transactions or users across Indian states and districts.
  - Click-to-drill from country to district level.

- **Transaction Insights**
  - Total transaction count and value by time period.
  - Top 10 districts and pincodes based on activity.

- **User Analytics**
  - Total registered users by state/district.
  - Top 10 locations by registered user count.

- **Transaction Categories**
  - Analyze transaction distribution by category (peer-to-peer, merchant, etc.)
  - Dual pie charts for count vs amount.

- **Mobile Brand Analysis**
  - Sunburst chart of user mobile brands by state, year, and quarter.

- **Dynamic SQL Backend**
  - Data fetched from MySQL based on user inputs.
  - State and quarter-based filters integrated with visualization.

---

##  Tech Stack

- **Languages & Tools**: Python, SQL  
- **Framework**: Streamlit  
- **Database**: MySQL  
- **Libraries**: 
  - `pandas` for data manipulation
  - `plotly` for rich, interactive charts
  - `streamlit_plotly_events` for interactivity
  - `rapidfuzz` for district name matching
  - `requests`, `json` for geojson handling

---

##  Sample Screenshots

###  Country Choropleth - Transactions
![Screenshot 2025-06-12 082234](https://github.com/user-attachments/assets/bfce5aa9-6e2e-42bb-baeb-0f75e6b334eb)


###  State Choropleth - Karnataka District View
![Screenshot 2025-06-12 082333](https://github.com/user-attachments/assets/13228dbe-db7e-4a97-b94d-8c710421e40c)


###  Transaction Categories (Pie Chart)
![Screenshot 2025-06-12 082355](https://github.com/user-attachments/assets/55f0ea8b-fc78-4be6-9646-dc4a879713df)


###  Top 10 Transaction Analysis
![Screenshot 2025-06-12 082418](https://github.com/user-attachments/assets/ac0f08fb-70ce-47a4-8523-62490176ee80)


###  Mobile Brand Distribution (Sunburst)
![Screenshot 2025-06-12 082525](https://github.com/user-attachments/assets/ae174c77-a6dc-4ff0-a9ce-78b4180a9fa0)



## Acknowledgements

- **Dataset Source**: PhonePe Pulse GitHub Repository
- **GeoJSON Maps**: Udit’s India Maps Dataset



