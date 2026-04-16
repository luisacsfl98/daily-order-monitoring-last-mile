# 🚀 Daily Order Monitoring – Last Mile Operations

## Project Overview

I developed an operational monitoring model for last-mile logistics, designed to track and prioritize **3,800+ daily orders in real time**.

The solution enables proactive decision-making by identifying critical orders **before SLA breaches occur**, shifting operations from reactive to controlled execution.

---

## Real-World Context

This project is based on a real logistics operation, handling high daily volumes across multiple regions.

It reflects real constraints such as:

- SLA-driven delivery commitments  
- Delivery vs Omnichannel operational flows  
- High volume of status transitions  
- Need for real-time prioritization  

This is **not a theoretical or academic model** — it was built to support real operational decisions.

---

## Objective

Enable daily operational prioritization by identifying:

- Orders at risk of SLA breach  
- Operational bottlenecks  
- Recoverable deliveries  
- Critical execution gaps  

---

## Data Architecture

The solution follows a structured pipeline:

```
TMS (CSV Source)
→ Power Query (ETL)
→ Data Model
→ Pivot Tables (Validation Layer)
→ Dashboard (Decision Layer)
```

---

## ETL Pipeline (Power Query – M Language)

All transformation logic is centralized in Power Query, ensuring consistency and eliminating manual Excel dependencies.

### Key transformations:

- Data type standardization  
- Column cleansing and normalization  
- Lead time calculation (aging logic)  
- Delivery vs Omnichannel classification  
- Status classification (Finalizado / Insucesso / Em Rota / Pendente)  
- SLA monitoring rules  

### Example:

```m
Duration.Days(Date.From(DateTime.LocalNow()) - [dt_aprovacao])
```

This approach ensures that **business logic is embedded in the data layer**, not in manual calculations.

---

## Data Validation & Trust Layer

A dedicated validation layer was implemented to ensure data reliability.

### Cross-System Validation (TMS)

- Total order volume reconciliation  
- Status consistency validation  
- Channel classification validation  

### Pivot-Based Validation Layer

- Total orders validation  
- Distribution validation by:
  - Status  
  - Order model (Delivery vs Omnichannel)  

This prevents incorrect insights caused by inconsistent or incomplete data.

---

## Data Governance

The process is standardized and documented through operational procedures, ensuring:

- Reproducibility  
- Controlled updates  
- Reduced manual dependency  
- Consistency across refresh cycles  

The architecture enforces a **single source of truth** through Power Query.

---

## Monitoring Logic

### Delivery (Process-Based)

- ≤ 3 days → On Track  
- 4–5 days → At Risk  
- ≥ 6 days → Critical  

### Omnichannel (SLA-Based)

- Within deadline → On Track  
- After deadline → Overdue  

A unified field (`Status monitoramento`) drives prioritization.

---

## Dashboard

The dashboard provides:

- Real-time operational visibility  
- SLA risk prioritization  
- Regional performance distribution  
- Status-based bottleneck identification  
- Order-level actionable view  

---

## Data Anonymization

To ensure confidentiality:

- Only dimensional fields were modified:
  - Pedido, Loja, Município, UF, Praça, Motorista, Transportadora, Expedidor  

- Operational fields were preserved:
  - Status  
  - Dates  
  - SLA logic  
  - Occurrences  

- Statistical distribution was maintained  

This results in a dataset that is:

- Realistic  
- Non-identifiable  
- Operationally consistent  

---

## Business Impact

- Reduced reaction time to critical orders  
- Enabled prioritization across thousands of daily records  
- Improved operational visibility  
- Standardized monitoring logic across regions  
- Increased confidence in decision-making through validated data  

---

## Tools & Skills

- Excel  
- Power Query (M Language)  
- Data Modeling  
- ETL Design  
- Pivot Tables  
- Operational Analytics  
- Data Validation & Governance  

---

## Language Note

The dashboard and data fields are in Portuguese, reflecting the original operational environment.

The documentation is provided in English to align with global data standards and increase accessibility.

---

## Key Takeaway

This project demonstrates how structured data pipelines, combined with validation and governance, can transform last-mile logistics from reactive execution into proactive operational control.
