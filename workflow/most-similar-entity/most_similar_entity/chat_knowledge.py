from dbgpt.core import (
    ChatPromptTemplate,
    HumanPromptTemplate,
    ModelMessage,
    ModelRequest,
    SystemPromptTemplate,
)
from dbgpt.core.awel import MapOperator
from dbgpt.experimental.intent.base import IntentDetectionResponse


class ChatKnowledgeOperator(MapOperator[ModelRequest, dict]):
    def __init__(self, task_name="chat_knowledge", **kwargs):
        super().__init__(task_name=task_name, **kwargs)

    async def map(self, input_value: ModelRequest) -> dict:
        from dbgpt._private.config import Config
        from dbgpt.configs.model_config import EMBEDDING_MODEL_CONFIG
        from dbgpt.rag.embedding.embedding_factory import EmbeddingFactory
        from dbgpt.storage.vector_store.base import VectorStoreConfig

        cfg = Config()

        user_input = input_value.messages[-1].content

        # 用"："分割user_input，第二部分为工时名称
        if "：" in user_input:
            entity_type = user_input.split("：")[0]
            entity_name_input = user_input.split("：")[1]
            if entity_type == "工时":
                knowledge_name = "working-hour"
            elif entity_type == "配件":
                knowledge_name = "fitting"
            elif entity_type == "故障":
                knowledge_name = "fault"
            else:
                raise ValueError("Knowledge name is required.")
        else:
            raise ValueError("Delimiter '：' not found in user input.")

        embedding_factory = self.system_app.get_component(
            "embedding_factory", EmbeddingFactory
        )
        from dbgpt.rag.retriever.embedding import EmbeddingRetriever
        from dbgpt.serve.rag.connector import VectorStoreConnector

        embedding_fn = embedding_factory.create(
            model_name=EMBEDDING_MODEL_CONFIG[cfg.EMBEDDING_MODEL]
        )

        config = VectorStoreConfig(
            name=knowledge_name,
            embedding_fn=embedding_fn,
        )
        vector_store_connector = VectorStoreConnector(
            vector_store_type=cfg.VECTOR_STORE_TYPE, vector_store_config=config
        )
        embedding_retriever = EmbeddingRetriever(
            top_k=5,
            index_store=vector_store_connector.client,
        )
        chunks = await embedding_retriever.aretrieve_with_scores(entity_name_input, 0.3)
        # 工时ID: 20000110 工时名称: 更换雨刮器电机总成
        result = {}
        if chunks:
            if entity_type == "工时":
                splits = chunks[0].content.split("工时名称: ")
                result['entity_name'] = splits[1]
                result['entity_id'] = splits[0].split("工时ID: ")[1].strip()
            elif entity_type == "配件":
                splits = chunks[0].content.split("配件名称: ")
                result['entity_name'] = splits[1]
                result['entity_id'] = splits[0].split("配件ID: ")[1].strip()
            elif entity_type == "故障":
                splits = chunks[0].content.split("故障名称: ")
                result['entity_name'] = splits[1]
                result['entity_id'] = splits[0].split("故障ID: ")[1].strip()

        return result
