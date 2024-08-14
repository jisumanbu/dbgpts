import asyncio
from datetime import datetime
from typing import AsyncIterator

from dbgpt.core import ModelRequest, ModelOutput
from dbgpt.core.awel import MapOperator, StreamifyAbsOperator
from dbgpt.experimental.intent.base import IntentDetectionResponse

_SHARE_DATA_DATABASE_NAME_KEY = "__database_name__"


class QueryFittingPriceOperator(StreamifyAbsOperator[ModelRequest, str]):
    def __init__(self, task_name="query_fitting_price_sql_executor", **kwargs):
        super().__init__(task_name=task_name, **kwargs)

    async def streamify(self, input_value: ModelRequest) -> AsyncIterator[str]:
        from dbgpt._private.config import Config
        from dbgpt.vis.tags.vis_chart import VisChart
        from .sql_templates import query_price_sql_template
        from .sql_templates import query_standard_fitting_price
        from .sql_templates import query_fitting_metadata

        yield "正在查询配件价格..."
        result = []

        start_time = datetime.now()
        print(f"QueryFittingPriceOperator.map start @ {datetime.now()}")
        cfg = Config()
        cfg.SYSTEM_APP = self.system_app
        shinwellvms_m = "shinwellvms_m"
        dw_shinwell = "dw_shinwell"

        # 从用户输入中解析必要入参
        ic: IntentDetectionResponse = input_value.context.extra.get("intent_detection")
        maint_order = ic.slots.get("Maintenance Order")
        fitting_name = ic.slots.get("Fitting Name")
        if not maint_order or not fitting_name:
            raise ValueError("缺失维保单号或配件名称")
        print(f"maint_order: {maint_order}, fitting_name: {fitting_name}")

        # 查询配件相关元数据
        time_before_get_connector = datetime.now()
        oltp_connector, olap_connector_1, olap_connector_2 = await asyncio.gather(
            self.blocking_func_to_async(cfg.local_db_manager.get_connector, shinwellvms_m),
            self.blocking_func_to_async(cfg.local_db_manager.get_connector, dw_shinwell),
            self.blocking_func_to_async(cfg.local_db_manager.get_connector, dw_shinwell)
        )
        print(f"Get connector cost: {datetime.now() - time_before_get_connector}")

        fitting_metadata_sql = query_fitting_metadata.format(maint_order=maint_order, fitting_name=fitting_name)
        fitting_metadata_df = await self.blocking_func_to_async(oltp_connector.run_to_df, fitting_metadata_sql)

        if fitting_metadata_df.empty:
            raise ValueError("未找到配件相关信息")

        vis = VisChart()
        fitting_metadata_chat = {
            "display_type": "response_table",
            "sql": fitting_metadata_sql.replace("\n", " "),
            "thoughts": ""
        }
        # 将column重命名
        #
        fitting_metadata_df_to_view = fitting_metadata_df.copy()
        fitting_metadata_df_to_view.drop(columns=['maint_order_no', 'fitting_id', 'service_agency_id'], inplace=True)
        fitting_metadata_df_to_view = fitting_metadata_df_to_view.rename(columns={
            'fitting_name': '配件名',
            'fitting_brand': '品牌',
            'fitting_model_name': '型号',
            'service_agency_name': '服务站',
            'service_station_city': '城市',
        })
        fitting_metadata_view = await vis.display(chart=fitting_metadata_chat, data_df=fitting_metadata_df_to_view)
        result.append("配件相关信息:")
        result.append(fitting_metadata_view)
        yield "\n".join(result)

        fitting_metadata = fitting_metadata_df.iloc[0].to_dict()
        query_price_sql = query_price_sql_template.format(**fitting_metadata)
        fitting_price_sql = query_standard_fitting_price.format(**fitting_metadata)

        price_df, standard_price_df = await asyncio.gather(
            self.blocking_func_to_async(olap_connector_1.run_to_df, query_price_sql),
            self.blocking_func_to_async(olap_connector_2.run_to_df, fitting_price_sql)
        )

        matrix_price_chat = {
            "display_type": "response_table",
            "sql": query_price_sql.replace("\n", " "),
            "thoughts": ""
        }

        if standard_price_df.empty:
            standard_price = "未找到该配件的型号库标准价"
        else:
            standard_price = standard_price_df.iloc[0]['standardPrice']

        result.append(standard_price)
        yield "\n".join(result)

        print(f"QueryFittingPriceOperator.map end @ {datetime.now()}, cost: {datetime.now() - start_time}")

        matrix_price_view = await vis.display(chart=matrix_price_chat, data_df=price_df)
        result.append(matrix_price_view)
        yield "\n".join(result)
