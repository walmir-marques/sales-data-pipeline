# 📊 Sales Data Pipeline — Projeto Completo de Análise de Dados Moderna

[![Dashboard Preview](Sales_Dashboard.png)](Sales_Dashboard.png)

---

## 🎯 **O Problema**

O mercado atual de **Data Analyst** está cada vez mais exigente: além de **Power BI, SQL e Excel**, recrutadores buscam profissionais que entendam **engenharia de dados básica** — ingestão automatizada, modelagem com **dbt**, orquestração com **Airflow** e pipelines end-to-end.

**Meu desafio:** Criar um **projeto portfólio real** que demonstrasse essas **skills modernas** sem sair da minha zona de analista, mas mostrando que eu **entendo e aplico** conceitos de Data Engineering.

## 💡 **A Solução**

Construí um **Sales Data Pipeline completo** que simula um fluxo de produção:

API Vendas → Bronze → Silver → Gold → Power BI Dashboard
↓ ↓ ↓ ↓ ↓
Airflow DAG orquestra tudo diariamente!

text

**Resultado:** Dashboard executivo com **KPIs de vendas** + **pipeline automatizado** + **qualidade garantida**.

## 🛠️ **Tech Stack Moderna**

🐍 Python + Pandas (ingestão)
🐳 Docker + Postgres (infra local)
🔄 dbt (modelagem Medallion Architecture)
🎼 Apache Airflow (orquestração)
📊 Power BI + DAX (visualização)
🧪 dbt tests (qualidade)

text

## 📋 **O que construí (9 Etapas reais)**

### **Etapa 1-3: Infra + Ingestão**

Docker Compose → Postgres + Airflow em 3 min
Python Requests → API vendas → Bronze (fct_orders)

text

### **Etapa 4-6: dbt Medallion Architecture**

Bronze: raw_orders (API as-is)
Silver: cleaned_orders (+PKs, nulls tratados)
Gold (5 tabelas):
├─ gold_monthly_revenue (receita, ticket médio)
├─ gold_customer_summary (LTV, retenção)
├─ gold_top_categories/products
└─ KPIs executivos prontos

text

### **Etapa 7: Power BI Dashboard**

4 KPI Cards (receita líquida 907k, ticket R$18k)
Gráfico linha receita (DAX "Receita Sem Gap")
Top categorias/produtos
Cores profissionais (#4a90c4, #dce8f4)

text

### **Etapa 8: Airflow DAG**

sales_pipeline_complete (@daily)
ingest_bronze → dbt_silver → dbt_gold → tests
Screenshot: [Airflow UI rodando ✅]

text

### **Etapa 9: Monitoramento**

schema.yml + dbt tests:

    unique(month)

    net_revenue >= 0

    avg_ticket > 0
    GitHub Actions CI/CD pronto

text

## 🎓 **O que aprendi (skills de mercado)**

✅ **dbt Medallion** = modelagem production-ready  
✅ **Airflow DAGs** = orquestração profissional  
✅ **DAX avançado** = dashboards sem gaps mágicos  
✅ **schema.yml + tests** = qualidade garantida  
✅ **Docker local** = deploy = produção  
✅ **Pipeline end-to-end** = recrutadores AMAM  

**Transição Analyst → Analyst 2.0:** Agora domino **engenharia básica** sem virar Data Engineer full-time.

## 🚀 **Resultado Final**

Pipeline 100% automatizado (API → BI)
Dashboard executivo com decisões reais
Monitoramento proativo (tests + alerts)
Portfólio production-grade para entrevistas

text

## 📱 **Demo Completa**

🔗 Power BI: Sales_Dashboard.pbix
🖥️ Airflow: localhost:8080/sales_pipeline_complete
📚 dbt Docs: localhost:8000

text

## 🗂️ **Estrutura do Projeto**

sales-pipeline/
├── airflow/dags/sales_pipeline_dag.py
├── models/schema.yml
├── models/gold/monthly_revenue.sql
├── extract.py (API → Bronze)
├── docker-compose.yml
├── Sales_Dashboard.pbix
└── README.md

text

## 🎯 **Status & Próximos passos**

✅ Parte 1: Local (Docker + Postgres)
🔄 Parte 2: Cloud (Snowflake + dbt Cloud)

text

---

⭐ **Data Analyst com skills de Data Engineering**  
**Walmir Marques** — Poços de Caldas, MG  
[LinkedIn](https://linkedin.com/in/walmir-marques) | [GitHub](github.com/seuuser)

---

**Status: Produção Local | Próximos: Snowflake + dbt Cloud**