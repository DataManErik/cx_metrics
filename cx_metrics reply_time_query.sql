SET @rowindex := -1;

#This "prep" CTE will convert the table's datatypes so the columns can be aggregated later. 
#reply_time and full_resolution_time are in minutes, the query divides these by 60 to convert to hours.
#The WHERE clause filters out a single row of null values.
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

#This query defines logic to find all relevant "reply_time" dimensions.
SELECT 
    ROUND(AVG(prep.reply_time),2) AS avg_reply_time,
    (#This subquery defines the logic needed to find "median_reply_time."
		SELECT
		ROUND(AVG(reply_med_prep.reply_time),2)
		FROM
		(
			SELECT
			@rowindex := @rowindex + 1 AS rowindex,
			reply_time
			FROM prep
			ORDER BY reply_time
		) as reply_med_prep
		WHERE
		reply_med_prep.rowindex IN (FLOOR(@rowindex / 2) , CEIL(@rowindex / 2))
	) AS median_reply_time,
    MIN(prep.reply_time) AS min_reply_time,
    MAX(prep.reply_time) AS max_reply_time,
    (#This subquery counts the number of records where both "reply_time" and "full_resolution_time" are 0.
		SELECT COUNT(*) 
		FROM prep 
        WHERE prep.reply_time =  0
        AND full_resolution_time = 0
	) AS count_zero_both_time,
	(#This subquery counts the number of records where "reply_time" is 0 but "full_resolution_time" is > 0.
		SELECT COUNT(*) 
		FROM prep 
        WHERE prep.reply_time =  0
        AND full_resolution_time > 0
	) AS count_zero_reply_but_some_resolution_time,
	COUNT(*) AS count_records
FROM prep;