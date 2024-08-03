from dbgpt.core import ModelRequest
from dbgpt.core.awel import MapOperator

_SHARE_DATA_DATABASE_NAME_KEY = "__database_name__"


class QueryFittingPriceOperator(MapOperator[ModelRequest, str]):
    def __init__(self, task_name="query_fitting_price", **kwargs):
        super().__init__(task_name=task_name, **kwargs)

    async def map(self, input_value: ModelRequest) -> str:
        from dbgpt._private.config import Config
        from dbgpt.datasource import RDBMSConnector
        from dbgpt.vis.tags.vis_chart import VisChart
        from .sql_templates import query_price_sql_template

        db_name = await self.current_dag_context.get_from_share_data(
            _SHARE_DATA_DATABASE_NAME_KEY
        )
        db_name = "dw_shinwell"
        vis = VisChart()
        cfg = Config()
        database: RDBMSConnector = cfg.local_db_manager.get_connector(db_name)
        sql = query_price_sql_template
        data_df = await self.blocking_func_to_async(database.run_to_df, sql)
        view = await vis.display(chart=input_value, data_df=data_df)
        return view
