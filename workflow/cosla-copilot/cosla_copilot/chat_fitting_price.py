import asyncio
from datetime import datetime

from dbgpt.core import ModelRequest
from dbgpt.core.awel import MapOperator
from dbgpt.experimental.intent.base import IntentDetectionResponse

_SHARE_DATA_DATABASE_NAME_KEY = "__database_name__"


class QueryFittingPriceOperator(MapOperator[ModelRequest, str]):
    def __init__(self, task_name="query_fitting_price_sql_executor", **kwargs):
        super().__init__(task_name=task_name, **kwargs)

    async def map(self, input_value: ModelRequest) -> str:
        from dbgpt._private.config import Config
        from dbgpt.vis.tags.vis_chart import VisChart
        from .sql_templates import query_price_sql_template
        from .sql_templates import query_standard_fitting_price
        from .sql_templates import query_fitting_metadata

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
        oltp_connector, olap_connector = await asyncio.gather(
            self.blocking_func_to_async(cfg.local_db_manager.get_connector, shinwellvms_m),
            self.blocking_func_to_async(cfg.local_db_manager.get_connector, dw_shinwell)
        )
        print(f"Get connector cost: {datetime.now() - time_before_get_connector}")

        fitting_metadata_df = await self.blocking_func_to_async(oltp_connector.run_to_df, query_fitting_metadata.format(maint_order=maint_order, fitting_name=fitting_name))

        if fitting_metadata_df.empty:
            raise ValueError("未找到配件相关信息")

        fitting_metadata = fitting_metadata_df.iloc[0].to_dict()
        query_price_sql = query_price_sql_template.format(**fitting_metadata)
        fitting_price_sql = query_standard_fitting_price.format(**fitting_metadata)

        price_df, standard_price_df = await asyncio.gather(
            self.blocking_func_to_async(olap_connector.run_to_df, query_price_sql),
            self.blocking_func_to_async(olap_connector.run_to_df, fitting_price_sql)
        )

        chart_to_display = {
            "display_type": "response_table",
            "sql": query_price_sql.replace("\n", " "),
            "thoughts": ""
        }

        if standard_price_df.empty:
            standard_price = "未找到该配件的型号库标准价"
        else:
            standard_price = standard_price_df.iloc[0]['standardPrice']

        print(f"QueryFittingPriceOperator.map end @ {datetime.now()}, cost: {datetime.now() - start_time}")

        vis = VisChart()
        price_view = await vis.display(chart=chart_to_display, data_df=price_df)
        return standard_price + "\n" + price_view
