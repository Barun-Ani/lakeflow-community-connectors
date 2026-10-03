\# Lakeflow OpenWeatherMap Community Connector



This connector ingests current weather data from the OpenWeatherMap API into Databricks using the Lakeflow Community Connector framework.



\## Prerequisites



\- An OpenWeatherMap account

\- An OpenWeatherMap API key

\- A Databricks workspace where you can create a Lakeflow community connector connection and ingestion pipeline

\- Network access from the pipeline environment to the OpenWeatherMap API



\## Setup



\### Required Connection Parameter



The connector requires one connection-level parameter:



| Name | Type | Required | Description |

|---|---|---|---|

| `api\_key` | string | Yes | API key used to authenticate with OpenWeatherMap. |



\### External Options



The connector supports the following table-level options:



```text

place\_name,latitude,longitude

