# Formula-1

Designed and implemented an end-to-end ETL data pipeline on Databricks, ingesting and transforming historical F1 race data (1950–2025) across 70+ seasons and 20+ constructor teams. Managed data assets through Databricks Catalog and orchestrated batch workflows via Databricks Jobs.

Built a modular PySpark transformation layer to engineer a custom composite performance score metric, enabling normalized cross-era driver comparisons. Results: Hamilton (Score: 1,968 | 106 wins, 7 titles) ranked ahead of Schumacher (1,765) and Verstappen (1,385). Stored transformed datasets as Delta tables for efficient downstream querying.

Applied SQL aggregations and PySpark to model the 2025 title race, revealing a three-way battle: Norris (423 pts), Verstappen (421 pts), and Piastri (410 pts) collectively accounting for 55%+ of all race wins, with McLaren's constructor dominance at 833 pts and 17 wins, double that of second-placed Mercedes (469 pts).

Delivered a 4-page Databricks SQL dashboard with season-level filters, donut charts, bar charts, and scatter plots presenting all-time dominance patterns, constructor standings, and 2025 race dynamics in a stakeholder-ready format.
