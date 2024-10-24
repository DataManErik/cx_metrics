# Drop the temporary table if it exists to avoid errors
DROP TEMPORARY TABLE IF EXISTS temp_prep;

# Create the temporary table 
CREATE TEMPORARY TABLE temp_prep AS
SELECT
    CAST(reply_time/60 AS DECIMAL(10,2)) AS reply_time,  -- Convert to hours
    CAST(full_resolution_time/60 AS DECIMAL(10,2)) AS full_resolution_time,  -- Convert to hours
    num_replies,
    is_rerouted,
    is_issue_solved,
    is_promoter,
    SUBSTRING_INDEX(manager, 'Ê', 1) AS manager -- Remove errant characters
FROM
    cx_metrics_2
WHERE
    id IS NOT NULL;

# Add an index to improve sorting and grouping performance
CREATE INDEX idx_manager_metrics 
ON temp_prep (manager, reply_time, full_resolution_time, num_replies, is_rerouted, is_issue_solved, is_promoter);

# Set group_concat_max_len session variable to ensure complete results
SET SESSION group_concat_max_len = 1000000;

# Calculate avg or median for each metric, group and order by manager
SELECT 
    manager,
    ROUND(AVG(reply_time), 2) AS avg_reply_time,
    SUBSTRING_INDEX(SUBSTRING_INDEX(
        GROUP_CONCAT(reply_time ORDER BY reply_time SEPARATOR ','),
        ',', 50 / 100 * COUNT(*) + 1), 
        ',', -1) AS median_reply_time,
    ROUND(AVG(full_resolution_time), 2) AS avg_full_resolution_time,
    SUBSTRING_INDEX(SUBSTRING_INDEX(
        GROUP_CONCAT(full_resolution_time ORDER BY full_resolution_time SEPARATOR ','),
        ',', 50 / 100 * COUNT(*) + 1), 
        ',', -1) AS median_full_resolution_time,
    ROUND(AVG(num_replies), 2) AS avg_num_replies,
    SUBSTRING_INDEX(SUBSTRING_INDEX(
        GROUP_CONCAT(num_replies ORDER BY num_replies SEPARATOR ','),
        ',', 50 / 100 * COUNT(*) + 1), 
        ',', -1) AS median_num_replies,
    ROUND(SUM(is_rerouted) / COUNT(*) * 100, 2) AS transfer_rate,  -- Percentage of cases transferred
    ROUND(SUM(is_issue_solved) / COUNT(*) * 100, 2) AS resolution_rate,  -- Percentage of cases marked resolved
    ROUND(SUM(is_promoter) / COUNT(*) * 100, 2) AS promoter_rate  -- Percentage of promoters    
FROM 
    temp_prep
GROUP BY 
    manager
ORDER BY 
    manager;  -- You can adjust the order if needed
