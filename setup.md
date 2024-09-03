# 最新的版本是0.5.10，但`pip install dbgpt`安装的总是0.5.9

* 在DB-GPT项目中，使用`pip install -e ".[default]"`安装最新的dbgpt
    ```shell
    cd ../DB-GPT
    conda activate dbgpts
    pip uninstall dbgpt
    pip install ".[openai]" -i https://pypi.tuna.tsinghua.edu.cn/simple \
      && pip install ".[default]" -i https://pypi.tuna.tsinghua.edu.cn/simple
    cd ../dbgpts
    ```
* 以可编辑模式安装了 dbgpt
  ```shell
  cd ../DB-GPT
  pip install -e .
  cd -
  ```

* repo
  ```shell
  dbgpt repo add --repo eosphoros/dbgpts --url https://github.com/eosphoros-ai/dbgpts.git
  dbgpt repo add --repo jisumanbu/dbgpts --url https://github.com/jisumanbu/dbgpts.git --branch v1.0
  dbgpt repo add --repo local/dbgpts --url /Users/jliu/git/ai/dbgpts
  ```

* run flow chat
  ```shell
  dbgpt run flow chat \
  --name cosla-copilot \
  --model "tongyi_proxyllm" \
  --stream \
  --messages "查询配件价格维保单号：712024081119529156，配件：空气滤清器/飞龙/3347"
  ```