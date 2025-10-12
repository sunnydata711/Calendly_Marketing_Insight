System Design for the Calendly Marketing Insight Project

1. My initial plan was to compare the costs of AWS Glue and EMR. Glue appeared cheaper and more suitable for this project, so I started by using Glue to create the first base dataset (Calendly_events) in Delta format.

2. I later encountered many issues with Glue’s Delta-format environment and suspected EMR would be more compatible with Delta, so I switched to EMR. As a result, I built the second base dataset (events_spend) on EMR. However, troubleshooting EMR was challenging due to the volume of logs and environment settings.

3. After finishing the two Delta-format base tables (Calendly_events and events_spend) with dt partitions and Delta logs, I tried two approaches to generate reporting tables—EMR and Athena. Both worked. For the final presentation, I used Athena because it let me easily query and verify the table results.

4. In a production setting, the best approach is likely to use EMR for everything—both the base tables (Calendly_events, events_spend) and the reporting tables—because it centralizes job and function management.

5. I learned a lot from this project: EMR, Delta Lake, Glue with Delta format, and Athena with Parquet.



