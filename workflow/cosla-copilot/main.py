import asyncio
import logging

from dbgpt.core.awel import CommonLLMHttpRequestBody

logger = logging.getLogger(__name__)


async def main():
    from cosla_copilot import dag

    req = CommonLLMHttpRequestBody(
        model="qwen-turbo",
        # model="qwen-max",
        # model="gpt-3.5-turbo",
        # messages="查询配件价格, 维保单号：712024072916256345， 配件：齿轮油/欧曼原厂/85W-90",
        messages="查询配件价格维保单号：712024081119529156，配件：空气滤清器/飞龙/3347",
        conv_uid="123456",
        stream=True,
        extra={
            # "space": "my_knowledge_space",
            # "db_name": "dbgpt_test",
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
