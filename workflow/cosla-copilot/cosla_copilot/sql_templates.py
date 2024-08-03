query_price_sql_template = """
/* a. 当前维保单对应的服务站该型号的历史最低价、平均价、最高价 */
SELECT 'a'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       dmcf.fitting_model_name,
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = 12746511 -- inputParam
  AND dmcf.fitting_name = '齿轮油'      -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂'   -- inputParam
  AND dmcf.fitting_model_name = '85W-90' -- inputParam
    union all
/* b. 当前维保单对应的服务站该配件同品牌的不同型号的历史最低价、平均价、最高价 */
SELECT 'b'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       '!= 85W-90'                                                            as fitting_model_name,
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = 12746511   -- inputParam
  AND dmcf.fitting_name = '齿轮油'        -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂'     -- inputParam
  AND dmcf.fitting_model_name != '85W-90' -- inputParam
union all
/* c. 当前维保单对应的服务站同市内的服务站该型号的历史最低价、平均价、最高价 */
SELECT 'c'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       dmcf.fitting_model_name,
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = '淮安市' -- inputParam
  AND dmcf.fitting_name = '齿轮油'         -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂'      -- inputParam
  AND dmcf.fitting_model_name = '85W-90'   -- inputParam
union all

/* d. 当前维保单对应的服务站同市内的服务站该配件同品牌的不同型号的的历史最低价、平均价、最高价 */
SELECT 'd'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       '!= 85W-90'                                                            as fitting_model_name, -- inputParam
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = '淮安市' -- inputParam
  AND dmcf.fitting_name = '齿轮油'         -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂'      -- inputParam
  AND dmcf.fitting_model_name != '85W-90'  -- inputParam
union all

/* e. 该型号系统内的历史最低价、平均价、最高价 */
SELECT 'e'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       dmcf.fitting_model_name,
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = '齿轮油'       -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂'    -- inputParam
  AND dmcf.fitting_model_name = '85W-90' -- inputParam
union all
/* f. 该配件同品牌的不同型号的历史最低价、平均价、最高价 */
SELECT 'f'                                                                    as group_index,
       dmcf.fitting_name,
       dmcf.service_agency_id,
       dmcf.service_station_city,
       '!= 85W-90'                                                            as fitting_model_name, -- inputParam
       dmcf.fitting_brand,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_min,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS price_avg,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS price_max
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = '齿轮油'    -- inputParam
  AND dmcf.fitting_brand = '欧曼原厂' -- inputParam
  AND dmcf.fitting_model_name != '85W-90' -- inputParam
;
"""