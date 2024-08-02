import asyncio
import logging

from dbgpt.core.awel import CommonLLMHttpRequestBody

logger = logging.getLogger(__name__)


async def main():
    from cosla_copilot import dag

    req = CommonLLMHttpRequestBody(
        model="qwen-max",
        messages="查询每个用户的订单数据, 数据库是default_sqlite",
        conv_uid="123456",
        stream=True,
        extra={
            # "space": "my_knowledge_space",
            "db_name": "dbgpt_test",
            # "tmp_dir_path": "/Users/jliu/git/ai/dbgpts/output",
            # "embedding_model": "/Users/jliu/git/ai/DB-GPT/models/text2vec-large-chinese/",
        },
    )
    dag_graph_file = dag.visualize_dag()
    if dag_graph_file:
        logger.info(f"Visualize DAG {str(dag)} to {dag_graph_file}")

    node = dag.leaf_nodes[0]
    async for out in await node.call_stream(req):
        print(out)


if __name__ == "__main__":
    from dbgpt.component import SystemApp
    from dbgpt.core.awel import DAGVar

    DAGVar.set_current_system_app(SystemApp())
    asyncio.run(main())
