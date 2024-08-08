from dbgpt.core import ModelRequest
from dbgpt.core.awel import MapOperator
from dbgpt.experimental.intent.base import IntentDetectionResponse

_SHARE_DATA_DATABASE_NAME_KEY = "__database_name__"


class QueryFittingPriceOperator(MapOperator[ModelRequest, str]):
    def __init__(self, task_name="query_fitting_price_sql_executor", **kwargs):
        super().__init__(task_name=task_name, **kwargs)

    async def map(self, input_value: ModelRequest) -> str:
        from dbgpt._private.config import Config
        from dbgpt.datasource import RDBMSConnector
        from dbgpt.vis.tags.vis_chart import VisChart
        from .sql_templates import query_price_sql_template
        from .sql_templates import query_standard_fitting_price
        from .sql_templates import query_fitting_metadata

        cfg = Config()
        cfg.SYSTEM_APP = self.system_app
        shinwellvms_m = "shinwellvms_m"
        dw_shinwell = "dw_shinwell"
        # db_name = await self.current_dag_context.get_from_share_data(
        #     _SHARE_DATA_DATABASE_NAME_KEY
        # )

        # 从用户输入中解析必要入参
        ic: IntentDetectionResponse = input_value.context.extra.get("intent_detection")
        maint_order = ic.slots.get("Maintenance Order")
        fitting_name = ic.slots.get("Fitting Name")
        if not maint_order or not fitting_name:
            raise ValueError("缺失维保单号或配件名称")
        print(f"maint_order: {maint_order}, fitting_name: {fitting_name}")

        # 查询配件相关元数据
        oltp: RDBMSConnector = cfg.local_db_manager.get_connector(shinwellvms_m)
        fitting_metadata_df = await self.blocking_func_to_async(oltp.run_to_df, query_fitting_metadata.format(maint_order=maint_order, fitting_name=fitting_name))

        if fitting_metadata_df.empty:
            raise ValueError("未找到配件相关元数据()")

        fitting_metadata = fitting_metadata_df.iloc[0].to_dict()
        query_price_sql = query_price_sql_template.format(**fitting_metadata)
        print(f"{query_price_sql}")

        olap: RDBMSConnector = cfg.local_db_manager.get_connector(dw_shinwell)
        price_df = await self.blocking_func_to_async(olap.run_to_df, query_price_sql)
        chart_to_display = {
            "display_type": "response_table",
            "sql": query_price_sql.replace("\n", " "),
            "thoughts": ""
        }
        print(chart_to_display)

        standard_price_df = await self.blocking_func_to_async(olap.run_to_df, query_standard_fitting_price.format(**fitting_metadata))
        if standard_price_df.empty:
            standard_price = "未找到该配件的型号库标准价"
        else:
            standard_price = standard_price_df.iloc[0]['standardPrice']

        vis = VisChart()
        price_view = await vis.display(chart=chart_to_display, data_df=price_df)
        return standard_price + "\n" + price_view
