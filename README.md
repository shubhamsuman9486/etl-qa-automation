# ETL QA Automation

An automation framework for **ETL data quality and validation testing**.  
This project provides SQL scripts and procedures to perform automated validation of ETL pipelines in data warehouse environments (e.g., Snowflake).  

---

## 📂 Repository Structure
- **Config Table.sql** → Stores test configurations and metadata.  
- **QA Automation Procedure.sql** → SQL procedures for executing data quality checks.  
- **Validation Results Table.sql** → Centralized table to store test execution results.  

---

## ✅ Features
- Automated validation of ETL processes.  
- Supports count checks, duplicate checks, null checks, structure check and full data comparisons.  
- Centralized logging of results for audit and reporting.  
- Reusable and configurable framework.  

---

## 🚀 How to Use
1. Create the **Config Table** to define table names and its primary/composite keys for mappings.  
2. Create the **Validation Results Table** to capture execution outcomes.  
3. Run the **QA Automation Procedure** to perform validations automatically.  

---

## 📊 Validation Types
- **Row Count Check**  
- **Duplicate Data Check**  
- **Null Value Check**  
- **Table Structure Check**  
- **Full Data Comparison**  

---

## 🔧 Tech Stack
- **SQL (Snowflake)**  
- **ETL QA Automation**  
- **Data Quality Testing**  

---

## 📌 Future Enhancements
- Integration with **Python + AWS Glue** for orchestration.  
- Dashboarding support (Power BI/Looker) for test result visualization.  
- CI/CD integration with GitHub Actions.  

---

## 👨‍💻 Author
**Shubham Suman**  
Automation Tester | Data QA | ETL Testing
