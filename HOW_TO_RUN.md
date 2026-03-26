# 🚀 How to Run

## Prerequisites

Make sure you have the following installed:
- [Docker Desktop](https://www.docker.com/products/docker-desktop)
- [Git](https://git-scm.com/)
- AWS account with S3 access

---

## 1. Clone the Repository
bash/Powershell
```
git clone https://github.com/yourusername/gov-data-pipeline.git
cd gov-data-pipeline
```

---

## 2. Set Up Environment Variables

Copy the example env file and fill in your values:

bash/Powershell
```
cp .env.example .env
```

Edit `.env`:
```env
KeyID=your_aws_access_key_id
AccessKey=your_aws_secret_access_key
bucket=your_s3_bucket_name
key=your_file.csv

DB_HOST=mysql
DB_USER=root
DB_PORT=3306
DB_PASSWORD=your_password
DB_NAME=govdata
```

---

## 3. Start the Containers

bash/Powershell
```
docker-compose up --build
```

Wait for all containers to start. You should see:
```
✔ Container govdata_mysql             Started
✔ Container govdata_airflow_webserver Started
✔ Container govdata_airflow_scheduler Started
```

---

## 4. Create Airflow Admin User

In a new terminal (bash/Powershell):
```
docker exec -it govdata_airflow_webserver airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

---

## 5. Open Airflow UI

Go to: [http://localhost:8080](http://localhost:8080)

Login with:
- **Username:** `admin`
- **Password:** `admin`

---

## 6. Trigger the Pipeline

1. Find `gov_data_pipeline` in the DAGs list
2. Click the toggle to unpause it
3. Click the ▶ button to trigger manually
4. Monitor progress in the logs

---

## 7. Verify Data in MySQL

Connect via MySQL Workbench:
- **Host:** `127.0.0.1`
- **Port:** `3307`
- **Username:** `root`
- **Password:** your password

Or via terminal (bash/Powershell):
```
docker exec -it govdata_mysql mysql -u root -pyourpassword
```
```sql
USE govdata;
SELECT COUNT(*) FROM staging_export_data;
SELECT COUNT(*) FROM fact_approved_deals;
SELECT COUNT(*) FROM fact_declined_deals;
```

---

## 8. Stop the Containers

bash/Powershell
```
docker-compose down
```

To also delete all data volumes:
```
docker-compose down -v
```

---

## 🔁 Re-running the Pipeline

Simply trigger the DAG again from the Airflow UI. The schema uses `CREATE TABLE IF NOT EXISTS` and `INSERT IGNORE` so re-runs won't duplicate data.
