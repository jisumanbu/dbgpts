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

        ic: IntentDetectionResponse = input_value.context.extra.get("intent_detection")
        maint_order = ic.slots.get("Maintenance Order")
        fitting_name = ic.slots.get("Fitting Name")
        if not maint_order or not fitting_name:
            raise ValueError("缺失维保单号或配件名称")
        print(f"maint_order: {maint_order}, fitting_name: {fitting_name}")

        sql = query_price_sql_template.format(maint_order=maint_order, fitting_name=fitting_name)
        print(f"sql: {sql}")

        # db_name = await self.current_dag_context.get_from_share_data(
        #     _SHARE_DATA_DATABASE_NAME_KEY
        # )
        db_name = "dw_shinwell"
        vis = VisChart()
        cfg = Config()
        cfg.SYSTEM_APP = self.system_app
        database: RDBMSConnector = cfg.local_db_manager.get_connector(db_name)
        data_df = await self.blocking_func_to_async(database.run_to_df, sql)
        chart_to_display = {
            "display_type": "response_table",
            "sql": sql.replace("\n", " "),
            "thoughts": ""
        }
        print(chart_to_display)
        view = await vis.display(chart=chart_to_display, data_df=data_df)

        standard_price_df = await self.blocking_func_to_async(database.run_to_df, query_standard_fitting_price.format(maint_order=maint_order, fitting_name=fitting_name))

        return standard_price_df.iloc[0]['standardPrice'] + "\n" + view
