# DP-750T00 Study Material: Implement Data Engineering Solutions using Azure Databricks

**Exam/Course:** DP-750T00-A  
**Certification:** Microsoft Certified: Azure Databricks Data Engineer Associate  
**Duration:** 4 days (96 hours) | **Level:** Intermediate  
**Role:** Data Engineer | **Subject:** Data Engineering  
**Product:** Azure Databricks  
**Last Updated:** July 13, 2026

---

## 📚 How to Use This Study Material

This isn't a study guide — it's **complete study material** that teaches you everything you need to know to pass the DP-750 exam. Each learning path file is a self-contained teaching resource with:

- **In-depth concept explanations** — not just bullet points, but the "why" behind each topic
- **Code examples with line-by-line explanations** — SQL, PySpark, and YAML you can actually practice with
- **Lab references woven in** — real scenarios from the 14 official labs to ground concepts in practice
- **Exam tips and common traps** — what the exam tests and how to avoid mistakes

### Suggested Study Order

```
1. Start with Learning Path 1 (foundations) → Do Labs 01-03
2. Move to Learning Path 2 (governance) → Do Labs 04-05
3. Learning Path 3 (data processing) → Do Labs 06-09
4. Learning Path 4 (pipelines, DevOps, tuning) → Do Labs 10-13
5. Review with lab walkthroughs if you need a refresher
```

---

## 📖 Study Materials

| File | Learning Path | Modules | Labs | Exam Weight |
|------|--------------|---------|------|-------------|
| [LP1: Set Up & Configure Environment](lp1-setup-and-configure-environment.md) | Set up and configure Azure Databricks environment | 1.1–1.5 | 01, 02, 03 | ~15-20% |
| [LP2: Secure & Govern Unity Catalog](lp2-secure-and-govern-unity-catalog.md) | Secure and govern Unity Catalog objects | 2.1–2.2 | 04, 05 | ~15-20% |
| [LP3: Prepare & Process Data](lp3-prepare-and-process-data.md) | Prepare and process data in Azure Databricks | 3.1–3.4 | 06, 07, 08, 09 | ~25-30% |
| [LP4: Deploy & Maintain Pipelines](lp4-deploy-and-maintain-pipelines.md) | Deploy and maintain data pipelines and workloads | 4.1–4.4 | 10, 11, 12, 13 | ~30-35% |
| [Lab Walkthroughs](lab-walkthroughs.md) | Detailed step-by-step walkthroughs for all 14 labs | All | 00–13 | Hands-on practice |

---

## 📋 Quick Reference

### Course Structure

| # | Learning Path | Modules | Labs |
|---|---------------|---------|------|
| 1 | Set Up and Configure Environment | 1.1 Explore Azure Databricks, 1.2 Architecture, 1.3 Integrations, 1.4 Compute, 1.5 Unity Catalog Objects | 01-03 |
| 2 | Secure and Govern Unity Catalog | 2.1 Security, 2.2 Governance | 04-05 |
| 3 | Prepare and Process Data | 3.1 Data Modeling, 3.2 Data Ingestion, 3.3 Data Transformation, 3.4 Data Quality | 06-09 |
| 4 | Deploy and Maintain Pipelines | 4.1 Pipeline Design, 4.2 Lakeflow Jobs, 4.3 Dev Lifecycle, 4.4 Monitor/Troubleshoot/Optimize | 10-13 |

### All Lab Scenarios at a Glance

| Lab | Scenario | Duration | Key Topics |
|-----|----------|----------|------------|
| 00 | Setup | 30 min | Provision Premium workspace, enable Unity Catalog |
| 01 | CityMoves Transit | 45 min | Workspace UI, upload CSV, Genie Code |
| 02 | HealthBridge Analytics | 30 min | Clusters, libraries, faker library, PySpark |
| 03 | Lakeside University | 45 min | Unity Catalog, medallion schemas, DDL, views, functions |
| 04 | NorthMart Retail | 35-40 min | Row filters, column masks, Key Vault secrets |
| 05 | AutoSphere AG | 30 min | Tags, VACUUM, lineage, audit logs |
| 06 | Northbank Financial | 45 min | SCD Type 2, liquid clustering, CDF, time travel |
| 07 | Solaris Energy | 45 min | COPY INTO, Auto Loader, CTAS |
| 08 | Pristine Properties | 45 min | Dedup, nulls, joins, PIVOT |
| 09 | ClearCover Insurance | 45 min | Lakeflow Pipelines, expectations, schema drift |
| 10 | GlobStay Hospitality | 45 min | Medallion pipeline, Lakeflow Jobs, If/else |
| 11 | TelConnect | 45 min | Lakeflow Jobs, triggers, cron, notifications |
| 12 | — | 45 min | Git version control, DAB, Databricks CLI, CI/CD |
| 13 | — | 45 min | Data skew, Spark UI, AQE, broadcast joins |

---

## 🔗 Official Resources

### Microsoft Learn
- **Course Page:** https://learn.microsoft.com/en-us/training/courses/dp-750t00
- **Certification Page:** https://learn.microsoft.com/en-us/credentials/certifications/implementing-data-engineering-solutions-using-azure-databricks/

### Learning Paths (Direct Links)
- **LP 1 - Setup & Configure:** https://learn.microsoft.com/en-us/training/paths/azure-databricks-data-engineer-set-up-configure-environment/
- **LP 2 - Secure & Govern:** https://learn.microsoft.com/en-us/training/paths/azure-databricks-data-engineer-secure-govern-unity-catalog/
- **LP 3 - Prepare & Process:** https://learn.microsoft.com/en-us/training/paths/azure-databricks-data-engineer-prepare-process-data/
- **LP 4 - Deploy & Maintain:** https://learn.microsoft.com/en-us/training/paths/azure-databricks-data-engineer-deploy-maintain-data-pipelines-workloads/

### Lab Exercises
- **GitHub Repo:** https://github.com/MicrosoftLearning/DP-750T00-Implement-Data-Engineering-Solutions-using-Azure-Databricks
- **Lab Instructions:** `Instructions/Labs/` in the repo
- **Notebook Solutions:** `Allfiles/` and `Allfiles/answers/` in the repo
- **Sample Data:** `Allfiles/data/` in the repo

### Documentation
- **Azure Databricks Documentation:** https://learn.microsoft.com/en-us/azure/databricks/
- **Unity Catalog Guide:** https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/
- **Delta Lake Documentation:** https://docs.delta.io/latest/index.html
- **Databricks CLI:** https://docs.databricks.com/en/dev-tools/cli/index.html

---

## 🎯 Exam Prep Strategy

1. **Read the learning path material** for each section
2. **Do the lab** that corresponds to that section (run the notebooks in your Databricks workspace)
3. **Review the lab walkthrough** if you need a refresher or didn't fully understand a step
4. **Test yourself** by explaining each concept out loud or writing the SQL from memory
5. **Focus on Pipelines & DevOps (30-35%)** — it's the heaviest-weighted section

### High-Weight Topics (Must-Know)
| Topic | Weight | Key Skills |
|-------|--------|------------|
| Lakeflow Jobs | High | Task DAGs, triggers, notifications, retry policies |
| DAB / CI/CD | High | Bundle YAML, Databricks CLI, Git folders, REST API deployment |
| Performance Tuning | High | AQE, broadcast joins, Spark UI diagnosis, data skew |
| Data Modeling | High | SCD Type 2, CDF, time travel, liquid clustering |
| Data Ingestion | High | Auto Loader, COPY INTO, MERGE, Structured Streaming |
| Unity Catalog Security | Medium | Row filters, column masks, privilege model, Key Vault |
| Unity Catalog Governance | Medium | Tags, lineage, audit logs, Delta Sharing |
| Compute Configuration | Medium | Cluster types, Photon, library management |

---

*Comprehensive study material compiled from Microsoft Learn course DP-750T00, official lab exercises, and exam requirements. Subject to Microsoft content updates.*
