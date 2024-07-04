# Phonepe Pulse Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly


**Introduction**


Project ' Phonepe Pulse Data Visualization and Exploration: A User-Friendly Tool Using Streamlit and Plotly ' is all about developing a user-friendly web application for cloning Live data from 'GitHub' a Repository system, later the data is cleaned and SQL Data Warehoused, All the process is executed using Python for Data Scraping the JSON data files from cloned location, MySQL for Data Warehounsing , 'Streamlit' a Python-based library for the Application development along with 'Plotly' for Python used to generate various types of Charts  and Maps for data visualization of the dataframe from 'Pandas' a Python Library.  

<br />

**Table of Contents**

1. Tools , Technologies and Skills
2. Installation
3. Utilisation
4. Features
5. Contributing
6. License
7. Contact

<br />

**Key Technologies and Skills**
- Python v3.12
- MySQL 8.0 
- Pandas 
- Streamlit
- Plotly


<br />

**Installation**

To run this project, you need to install the following packages:
```python
pip install os-sys
pip install python-git
pip install pypi-json
pip installmysql-connector-python
pip install pandas
pip install regex
pip install streamlit
pip install plotly-express
```

<br />

**Utilisation**

To use this project, follow these steps:

1. Clone the repository: ```git clone https://github.com/harish-kumar-kp/Phonepe-Pulse-Data-Visualization-and-Exploration.git```
2. Install the required packages and libraries : ```pip install -r requirements.txt```
3. Launch the App by selecting the local path where this Git Hub Repository is cloned and later run in Command prompt with ```streamlit run PhonePe_App.py```
4. Access the app in your browser at ```http://localhost:8501```

<br />

**Features**

**Git Hub Data Cloning:**With Python coding The GitHub repository of accumulated dataset is cloned in a designated location of the local path in the Computer only if it is not present properly so we can avoid recursive cloning.

**Data Harvesting:** With Python coding the cloned dataset in the local path is scraped by navigating through catogories of folders namely Aggragated, Map and Top and their Sub Folders namely Insurance, Transaction and Users by their State wise, then Year wise and later Quarter Wise and loading and reading the Quarterly JSON files iteratively and stored in ' My SQL ' Tables if only non of the are created Tables so as to avoid the data duplication in database.

**JSON Files to SQL Tables:** The application allows users to migrate data from JSON Files to a SQL data warehouse.To ensure compatibility with a structured format, the data is cleansed using the powerful pandas library. Following data cleaning, the information is segregated into separate tables namely Aggragated, Map and Top and their sub catogories of Insurance, Transaction and Users making a mix and match of 9 Tables. 

**Question driven Data Exploratory and Visualization:** The Feature provides comprehensive data analysis capabilities with Streamlit with predefined Queries  complete Data Exploration. With Plotily the users can create interactive charts, graphs and maps from the collected data.

**Data Insights:** The reasoning abilities based on the scenarios of the output of the queries achieved by archived information on Geographical Parameters like State and District and the Major Contributing City and Citi's Nature.

**Customised Data Exploratory and Visualization: **The project Feature provides comprehensive data analysis capabilities with Streamlit with Custom Query Bulding capability for complete Data Exploration. With Plotly the users can create interactively Queries in MySQL that creates charts, graphs and maps from the collected data in a mix and match methodology with internal parameter comparisons.

🎬 𝗣𝗿𝗼𝗷𝗲𝗰𝘁 𝗗𝗲𝗺𝗼 𝗩𝗶𝗱𝗲𝗼: [https://www.youtube.com/watch?v=jScf-Qh39jI&t=238s](https://www.youtube.com/watch?v=jScf-Qh39jI&t=238s)

<br />

**Contributing**

Contributions to this project are welcome! If you encounter any issues or have suggestions for improvements, please feel free to submit a pull request. Your Valuable Suggestions are always invited happily.

<br />

**License**

This project is licensed under the CDLA-Permissive-2.0 open data License. Please review the LICENSE file for more details.

<br />

**Contact**

📧 Email: harishk_kotte@rediffmail.com

🌐 LinkedIn: [https://www.linkedin.com/in/harish-kumar-k-p-67587a262/](https://www.linkedin.com/in/harish-kumar-k-p-67587a262/)

For any further questions or inquiries, feel free to reach out. We are happy to assist you with any queries.

