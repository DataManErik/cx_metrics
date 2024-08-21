#This query will convert the table's datatypes so the columns can be aggregated later. 
#reply_time and full_resolution_time are in minutes, the query divides these by 60 to convert to hours.
#SUBSTRING_INDEX is used to remove errant characters from the agent and manager dimensions.	
#The WHERE clause filters out the single row of null values.

WITH prep AS 
(
SELECT
	CAST(reply_time/60 AS DECIMAL(10,2)) AS reply_time, #Rounding to 1 decimal resulted in zero values for some records
	CAST(full_resolution_time/60 AS DECIMAL(10,2)) AS full_resolution_time, #Rounding to 1 decimal resulted in zero values for some records
    CAST(num_replies AS SIGNED) AS num_replies, #MySQL requires either 'SIGNED' or 'UNSIGNED' for the CAST function to produce an integer.
	CAST(is_rerouted AS SIGNED) AS is_rerouted,
    CAST(is_issue_solved AS SIGNED) AS is_issue_solved,
	CAST(is_promoter AS SIGNED) AS is_promoter,
	CAST(group_number AS SIGNED) AS group_number,
    SUBSTRING_INDEX(agent, 'Ê', 1) AS agent,
    SUBSTRING_INDEX(manager,'Ê', 1) AS manager
FROM cx_metrics_2
WHERE id IS NOT NULL
)

SELECT 
	CAST(SUM(is_issue_solved)/COUNT(*) AS DECIMAL(10,2)) AS resolution_rate
FROM prep; 