# Drop the temporary table if it exists to avoid errors
DROP TEMPORARY TABLE IF EXISTS temp_prep;

# Create the temporary table 
# This CTE will convert the table's datatypes so the columns can be aggregated later
# reply_time and full_resolution_time are in minutes, the query divides these by 60 to convert to hours
# SUBSTRING_INDEX is used to remove errant characters from the agent and manager dimensions    
# The WHERE clause filters out a single row of null values

CREATE TEMPORARY TABLE temp_prep AS
SELECT
    CAST(reply_time/60 AS DECIMAL(10,2)) AS reply_time,
    SUBSTRING_INDEX(manager, 'Ê', 1) AS manager
FROM
    cx_metrics_2
WHERE
    id IS NOT NULL;

# Add an index to improve sorting and grouping performance
CREATE INDEX idx_manager_reply_time ON temp_prep (manager, reply_time);

# Initialize session variables for tracking
SET @current_manager := NULL, @current_index := 0, @total_rows := 0;

# Set group_concat_max_len session variable to ensure complete results
SET SESSION group_concat_max_len = 1000000;

# Calculate median, average, min, max, and count, and order by median_reply_time
# The GROUP_CONCAT function strings all of the reply_time records together, sorts by ascending order, and allows the SUBSTRING_INDEX function to select the median

SELECT 
    manager,
    avg_reply_time,
    median_reply_time,
    min_reply_time,
    max_reply_time,
    count_records
FROM (
    SELECT 
        manager,
        ROUND(AVG(reply_time),2) AS avg_reply_time,
        SUBSTRING_INDEX(SUBSTRING_INDEX(
            GROUP_CONCAT(reply_time ORDER BY reply_time SEPARATOR ','),
            ',', 50 / 100 * COUNT(*) + 1), 
            ',', -1) AS median_reply_time,
        MIN(reply_time) AS min_reply_time,
        MAX(reply_time) AS max_reply_time,
        COUNT(*) AS count_records
    FROM (
        SELECT 
            manager, 
            reply_time
        FROM 
            temp_prep
        ORDER BY 
            reply_time
    ) AS sorted_replies
    GROUP BY 
        manager
) AS results
ORDER BY 
    CAST(median_reply_time AS DECIMAL(10,2)) ASC;  # Assuming median_reply_time is a string, cast to DECIMAL for numerical sorting
