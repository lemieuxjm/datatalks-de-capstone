# DataTalksClub Data Engineering Zoomcamp Capstone Project

This README.md will provide overview of project and instructions for reproducing the environment and project for review purposes

---

## Prerequisites

For GCP
 - setup a gcp project, location, and service account
 - identify a storage bucket name

For Kestra
 - set up an .env_encoded file for the gcp service account

Clone the repository at:
 - enter repo link here

---

## Project Discussion

 - Describe data source & why chosen
 - Describe dashboard & what insights can be derived from it
 - Describe process from source -> dashboard 

---


## Contents

 - enter visual for repo structure here

---

## Instructions

1. **Open Github codespace in VSCode**

2. **In the terminal run:**  --note that a change is needed to this note

```bash
cd xxxxxxxx
docker compose up -d
```

3. **Access Kestra at** [http://localhost:8080](http://localhost:8080).

4. **Set up the secret for the gcp service account** [Add Service Account as a Secret](https://kestra.io/docs/how-to-guides/google-credentials#add-service-account-as-a-secret)

5. **(Optional) Run sync-to-git flow**

6. **Verify these flows are present**  --note that changes are needed to this note

   - 00_setup_kv
   - 01_verify_gcp_setup
   - 02_pipeline1
   - 03_pipeline2
   - 04_pipeline3

7. **Edit 06_gcp_kv.yaml with your values**

8. **Run the flow for** `00_setup_kv.yaml`
   - change this to auto run the kv setup flow

9. **Run the flow for** `01_verify_gcp_setup.yaml` 
   - change this to auto run the gcp setup flow
   - Verify the dataset is present in BigQuery   --can this be done via code?
   - Verify the storage bucket is present in Storage --can this be done via code?

10. **Run the flow for** `02_pipeline1.yaml`  
   - change this to auto run pipeline 1 thru Kestra setup/save this setup
   - Pipeline 1 extracts data from source to gcs
   - Run verification steps   

11. **Run the flow for** `03_pipeline2.yaml` 
   - change this to auto run pipeline 2 thru Kestra setup/save this setup
   - Pipeline 2 loads gcs to raw BQ
   - Run verification steps     

12. **Run the flow for** `04_pipeline3.yaml` 
   - change this to auto run pipeline 3 thru Kestra setup/save this setup
   - Pipeline 3 transforms raw BQ to silver BQ  
   - Pipeline 3 also transforms silver BQ to gold BQ     
   - Run verification steps     

13. **Look at Dashboard**   
   - Provide link
   - Identify how to interact and what to review

14. **Shutting Down**         
   - Provide steps to shut down
