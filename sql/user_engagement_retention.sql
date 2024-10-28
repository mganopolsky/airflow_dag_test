WITH 
year_month_subset AS (
    SELECT 
        DATE_TRUNC('month', activity_timestamp) AS year_month,
        user_id,        
        product_id,
        activity_type
    FROM user_activity_log
    where product_id is not null
),
monthly_activities AS (
    SELECT 
        year_month,
        user_id,
        COUNT(*) AS total_activities,
        COUNT(DISTINCT product_id) AS total_views        
    FROM year_month_subset
    group by 
        year_month,
        user_id
),
monthly_active_users AS (
    -- Monthly Active Users (MAU)
    SELECT 
        year_month,
        COUNT(DISTINCT user_id) AS MAU
    FROM monthly_activities
    GROUP BY year_month
),
new_users_by_month AS (
    -- Identify first-time users in each month
    SELECT 
        user_id,
        MIN(DATE_TRUNC('month', activity_timestamp)) AS first_activity_month
    FROM user_activity_log
    GROUP BY user_id
),
monthly_retention AS (
    -- Calculate retention by checking if new users return the following month
    SELECT 
        nm.first_activity_month AS current_month,
        COUNT(DISTINCT nm.user_id) AS new_users,
        COUNT(DISTINCT urm.user_id) AS returning_users
    FROM new_users_by_month nm
    LEFT JOIN user_activity_log urm 
        ON nm.user_id = urm.user_id
        AND DATE_TRUNC('month', urm.activity_timestamp) = DATEADD('month', 1, nm.first_activity_month)
    GROUP BY nm.first_activity_month
),
avg_activities_per_user AS (
    -- Average Monthly Activities per User
    SELECT 
        year_month,
        AVG(total_activities) AS avg_activities_per_user
    FROM monthly_activities
    GROUP BY year_month
),
popular_products AS (
    -- Top Popular Products by Views
    SELECT 
        year_month,
        product_id,
        COUNT(DISTINCT user_id) AS unique_views,
        ROW_NUMBER() OVER (PARTITION BY year_month ORDER BY COUNT(DISTINCT user_id) DESC) AS rank
    FROM year_month_subset
    WHERE activity_type = 'view_product'
    GROUP BY year_month, product_id
),
top_3_popular_products AS (
    -- Ensure unique_views is available for ordering
    SELECT 
        year_month,
        product_id,
        unique_views
    FROM popular_products
    WHERE rank <= 3
),
top_3_products_array AS (
    -- Create a subquery for ARRAY_AGG with proper ordering
    SELECT 
        year_month,
        -- a list of top 3 products
        ARRAY_AGG(product_id) WITHIN GROUP (ORDER BY unique_views DESC) AS top_3_products,
        -- a list if view count of top 3 products in previous column
        ARRAY_AGG(unique_views) WITHIN GROUP (ORDER BY unique_views DESC) AS top_3_view_counts,
    FROM top_3_popular_products
    GROUP BY year_month
)
-- Final query combining all requirements
SELECT 
    mau.year_month,
    mau.MAU,
    COALESCE(rn.returning_users * 100.0 / NULLIF(rn.new_users, 0), 0) AS retention_rate,
    aa.avg_activities_per_user,
    tp.top_3_products,
    tp.top_3_view_counts
FROM monthly_active_users mau
LEFT JOIN monthly_retention rn ON mau.year_month = rn.current_month
LEFT JOIN avg_activities_per_user aa ON mau.year_month = aa.year_month
LEFT JOIN top_3_products_array tp ON mau.year_month = tp.year_month
ORDER BY mau.year_month;