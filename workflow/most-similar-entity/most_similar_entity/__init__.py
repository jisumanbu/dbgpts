"""all-in-one-entrance flow package"""
import os
from typing import List

from dbgpt.core import (
    BaseMessage,
    InMemoryStorage,
    ModelMessage,
    ModelRequest,
    StorageConversation,
    StorageInterface, )
from dbgpt.core.awel import DAG, JoinOperator, MapOperator, is_empty_data
from dbgpt.core.awel.trigger.http_trigger import (
    CommonLLMHttpRequestBody,
    CommonLLMHttpTrigger,
)
from dbgpt.core.operators import BaseConversationOperator
from dbgpt.model.operators import LLMOperator
from dbgpt.model.proxy.llms.chatgpt import OpenAILLMClient
from dbgpt.model.proxy.llms.tongyi import TongyiLLMClient

from .chat_knowledge import ChatKnowledgeOperator


class RequestHandleOperator(
    BaseConversationOperator, MapOperator[CommonLLMHttpRequestBody, ModelRequest]
):
    def __init__(self, storage: StorageInterface, **kwargs):
        MapOperator.__init__(self, **kwargs)
        BaseConversationOperator.__init__(
            self, storage=storage, message_storage=storage
        )

    async def map(self, input_value: CommonLLMHttpRequestBody) -> ModelRequest:
        # Create a new storage conversation, this will load the conversation from
        # storage, so we must do this async
        storage_conv: StorageConversation = await self.blocking_func_to_async(
            StorageConversation,
            conv_uid=input_value.conv_uid,
            chat_mode=input_value.chat_mode,
            user_name=input_value.user_name,
            sys_code=input_value.sys_code,
            conv_storage=self.storage,
            message_storage=self.message_storage,
            param_type="",
            param_value=input_value.chat_param,
        )
        # Get history messages from storage
        history_messages: List[BaseMessage] = storage_conv.get_history_message()
        messages = ModelMessage.from_base_messages(history_messages)
        messages.append(ModelMessage.build_human_message(input_value.messages))

        # Save the storage conversation to share data, for the child operators
        await self.current_dag_context.save_to_share_data(
            self.SHARE_DATA_KEY_STORAGE_CONVERSATION, storage_conv
        )
        await self.current_dag_context.save_to_share_data(
            self.SHARE_DATA_KEY_MODEL_REQUEST_CONTEXT, input_value
        )
        return ModelRequest.build_request(input_value.model, messages)


def join_func(*args):
    for arg in args:
        if not is_empty_data(arg):
            return arg
    return None


with DAG("dbgpts_find_most_similar_entity_dag") as dag:
    trigger = CommonLLMHttpTrigger(
        "/dbgpts/most-similar-entity",
        methods="POST",
        streaming_predict_func=lambda x: x.stream,
    )
    storage = InMemoryStorage()
    request_handle_task = RequestHandleOperator(storage)
    chat_knowledge_task = ChatKnowledgeOperator()
    join_task = JoinOperator(combine_function=join_func)
    # llm_client_quick = OpenAILLMClient(
    #     model_alias="gpt-4o-mini",
    #     # api_base=os.getenv("OPENAI_API_BASE"),
    #     api_base="http://openai-proxy-openai-proxy-qaauardwwh.us-west-1.fcapp.run/v1",
    #     api_key=os.getenv("OPENAI_API_KEY"),
    # )
    from dbgpt._private.config import Config
    cfg = Config()
    llm_client_quick = TongyiLLMClient(model="qwen-turbo", api_key=cfg.tongyi_proxy_api_key)

    trigger >> request_handle_task

    # Chat knowledge task
    (
            request_handle_task
            >> chat_knowledge_task
            >> LLMOperator(llm_client_quick)
            >> join_task
    )
