query_fitting_metadata = """
  select a.maint_order_no,
       msf0.fitting_id,
       hf.fitting_name         AS fitting_name,
       msf0.fitting_brand_name AS fitting_brand,
       hf.fitting_model_name   AS fitting_model_name,
       station.agency_id       AS service_agency_id,
       station.agency_name       AS service_agency_name,
       ifnull(ifnull((select city.city_name
                      from city_code dis
                               left join city_code city on city.city_code = CONCAT(left(dis.city_code, 4), '00')
                      where cast(right(dis.city_code, 2) as int) > 0
                        and dis.city_code = llc.city_code),
                     (select cc1.city_name
                      from city_code cc1
                      where cc1.city_code = llc.city_code)),
              '未知')          AS service_station_city
from maint_order a
         left join agency station on station.agency_type = 'STATION' and a.dispatched_station_id = station.agency_id
         left join location llc on llc.location_id = station.location_id
         left join maint_order_fee e on a.maint_order_no = e.maint_order_no
         left join maint_vehicle_fitting hf on e.maint_workinghour_fitting_id = hf.maint_workinghour_fitting_id
         left join maint_fitting_snapshot as msf0 on msf0.maint_workinghour_fitting_id = e.maint_workinghour_fitting_id
where a.maint_order_no = '{maint_order}'
  and station.agency_name not like '%测试%'
  and station.agency_name NOT LIKE 'Test%'
  and e.maint_fee_type = 'FITTING_COST'
  and (concat_ws('/', msf0.fitting_name, msf0.fitting_brand_name, if(msf0.fitting_model_name = '其他', null, msf0.fitting_model_name), msf0.fitting_model_remark) = '{fitting_name}'
    or concat_ws('/', msf0.fitting_name, hf.fitting_description) = '{fitting_name}')
"""

query_price_sql_template = """
SELECT '同品牌同型号，当前服务站'                                              as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       dmcf.service_station_city                                              as 城市,
       dmcf.service_station_name                                              as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       dmcf.fitting_model_name                                                as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = '{service_agency_id}'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name = '{fitting_model_name}'
union all
SELECT '同品牌不同型号，当前服务站'                                            as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       dmcf.service_station_city                                              as 城市,
       dmcf.service_station_name                                              as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       concat('非', dmcf.fitting_model_name)                                  as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.service_agency_id = '{service_agency_id}'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name != '{fitting_model_name}'
union all
SELECT '同品牌同型号，同市'                                                    as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       dmcf.service_station_city                                              as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       dmcf.fitting_model_name                                                as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = '{service_station_city}'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name = '{fitting_model_name}'
union all
SELECT '同品牌不同型号，同市'                                                  as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       dmcf.service_station_city                                              as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       concat('非', dmcf.fitting_model_name)                                  as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  and dmcf.service_station_city = '{service_station_city}'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name != '{fitting_model_name}'
union all

SELECT '同品牌同型号，所有'                                                    as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       ''                                                                     as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       dmcf.fitting_model_name                                                as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name = '{fitting_model_name}'
union all
SELECT '同品牌不同型号，所有'                                                  as 维度,
       cast(
               min(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最低价,
       cast((
           sum(dmcf.fee_price) / sum(dmcf.accepted_count)) AS DECIMAL(18, 2)) AS 平均价,
       cast(
               max(dmcf.fee_price / dmcf.accepted_count) AS DECIMAL(18, 2))   AS 最高价,
       ''                                                                     as 城市,
       ''                                                                     as 服务站,
       dmcf.fitting_name                                                      as 配件,
       dmcf.fitting_brand                                                     as 品牌,
       concat('非', dmcf.fitting_model_name)                                  as 型号
FROM dwd_maint_order_accepted_cost_fee_f dmcf
WHERE dmcf.completed_time > '2023-01-01'
  AND dmcf.maint_fee_type = '配件费'
  AND dmcf.fitting_name = '{fitting_name}'
  AND dmcf.fitting_brand = '{fitting_brand}'
  AND dmcf.fitting_model_name != '{fitting_model_name}'
ORDER BY 维度
"""

query_standard_fitting_price = """
select concat('型号库标准价：￥', standard_price) as standardPrice
from dim_standard_fitting_model_scd
where fitting_id = '{fitting_id}'
  and fitting_brand_name = '{fitting_brand}'
  and fitting_model_name = '{fitting_model_name}'
"""