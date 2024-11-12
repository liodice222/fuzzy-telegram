# fuzzy-telegram
uploading python scripts to private instance for DEP


This repo was used in a private subnet in OCI to interact with Oracle Object Storage via private endpoint and Autonomous Data Warehouse to execute an ETL Data Pipeline. 
Python was used to: 
1) Create SQLPlus tables
2) Extract csvs from object storage to be stored on the private instance within the private subnet
3) Load data from csvs on instance onto ADW using sqlplus
4) Verify data was loaded and then upload new csvs to Object Storage via private endpoint
5) Remove csvs from private instance
6) Data Visualization was completed on this repo pulling from ADW https://github.com/liodice222/ubiquitous-train-deploy-jupyter 
[Data_Engineering_Proj_ERD.pdf](https://github.com/user-attachments/files/17719830/Data_Engineering_Proj_ERD.pdf)
