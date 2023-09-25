## PURPOSE
Sparkify generates more events data and songs data with passing time. For enhanced analytics downstream it needs to setup data pipelines which runs periodically. In this project we do this using Airflow.
We will create custom operators since some of the datasets will under go similar transforms. The diffrences in these tasks will be the parameters of the operators.
First, tables are created in Redshift, then events and songs data is staged, then rows are inserted in the fact table 'songsplay'. Using the fact table we insert rows in the dimension tables- songs, artists, users, time. Finally a quality check is done on all of the tables.

## HOW TO USE
1. Create all tables in Redshift.
2. In the terminal run '/opt/airflow/start.sh'. Then click on 'ACCESS AIRFLOW' to access its UI.
3. Turn the toggle on for 'udac_example_dag.py'