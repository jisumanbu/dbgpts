query_price_sql_template = """
  with conditions as (SELECT service_station_city,
                           service_agency_id,
                           maint_order_no,
                           fitting_name,
                           fitting_brand,
                           fitting_model_name
                    FROM dwd_maint_order_accepted_cost_fee_f
                    WHERE maint_order_no = '{maint_order}'
                      and concat_ws('/', fitting_name, fitting_brand, fitting_model_name) = '{fitting_name}')
SELECT 'a. 当前维保单对应的服务站该型号的历史最低价、平均价、最高价'            as 维度,
       dmcf.service_station_city                                              as 城市,
       dmcf.service_station_name                                              as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       dmcf.fitting_model_name                                                as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = (select service_agency_id from conditions)
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name = (select fitting_model_name from conditions)
union all
SELECT 'b. 当前维保单对应的服务站该配件同品牌的不同型号的历史最低价、平均价、最高价' as 维度,
       dmcf.service_station_city                                                   as 城市,
       dmcf.service_station_name                                                   as 服务站,
       dmcf.fitting_name                                                           as 配件,
       dmcf.fitting_brand                                                          as 品牌,
       concat('非', dmcf.fitting_model_name)                                         as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))        AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2))      AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))        AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = (select service_agency_id from conditions)
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name != (select fitting_model_name from conditions)
union all
SELECT 'c. 当前维保单对应的服务站同市内的服务站该型号的历史最低价、平均价、最高价' as 维度,
       dmcf.service_station_city                                                 as 城市,
       ''                                                                        as 服务站,
       dmcf.fitting_name                                                         as 配件,
       dmcf.fitting_brand                                                        as 品牌,
       dmcf.fitting_model_name                                                   as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))      AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2))    AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))      AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = (select service_station_city from conditions)
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name = (select fitting_model_name from conditions)
union all
SELECT 'd. 当前维保单对应的服务站同市内的服务站该配件同品牌的不同型号的的历史最低价、平均价、最高价' as 维度,
       dmcf.service_station_city                                                                   as 城市,
       ''                                                                                          as 服务站,
       dmcf.fitting_name                                                                           as 配件,
       dmcf.fitting_brand                                                                          as 品牌,
       concat('非', dmcf.fitting_model_name)                                         as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))                        AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2))                      AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))                        AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = (select service_station_city from conditions)
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name != (select fitting_model_name from conditions)
union all

SELECT 'e. 该型号系统内的历史最低价、平均价、最高价'                            as 维度,
       ''                                                                     as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       dmcf.fitting_model_name                                                as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name = (select fitting_model_name from conditions)
union all
SELECT 'f. 该配件同品牌的不同型号的历史最低价、平均价、最高价'                  as 维度,
       ''                                                                     as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       concat('非', dmcf.fitting_model_name)                                         as 型号,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = (select fitting_name from conditions)
  AND dmcf.fitting_brand = (select fitting_brand from conditions)
  AND dmcf.fitting_model_name != (select fitting_model_name from conditions)
ORDER BY 维度
"""

query_standard_fitting_price = """
with conditions as (SELECT service_station_city,
                           service_agency_id,
                           maint_order_no,
                           fitting_id,
                           fitting_name,
                           fitting_brand,
                           fitting_model_name,
                           fitting_model_id
                    FROM dwd_maint_order_accepted_cost_fee_f
                    WHERE maint_order_no = '{maint_order}'
                      and concat_ws('/', fitting_name, fitting_brand, fitting_model_name) = '{fitting_name}')
select concat('型号库标准价：￥', standard_price) as standardPrice
from dim_standard_fitting_model_scd
where fitting_id = (select fitting_id from conditions)
  and fitting_brand_name = (select fitting_brand from conditions)
  and fitting_model_name = (select fitting_model_name from conditions)
"""