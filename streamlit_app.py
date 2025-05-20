# Import python packages
import _snowflake
import json
import streamlit as st
from snowflake.snowpark.context import get_active_session


DATABASE = "KYOTO_PORTA_CORTEX_SEARCH_ANALYST_DOCS"
SCHEMA = "PORTA_ANALYST"
STAGE = "DOCS_STAGE"
FILE = "porta_analyst_semantic_model.yaml"

slide_window = 5 # 記憶する過去の会話の数

def config_options():
    st.sidebar.button("やり直す", key="clear_conversation", on_click=init_messages)

def init_messages():
    # Initialize chat history
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.suggestions = []
        st.session_state.active_suggestion = None

def get_prompt_history(question):

    chat_history = []
    
    start_index = max(0, len(st.session_state.messages) - slide_window)
    for i in range (start_index , len(st.session_state.messages) -1):
         chat_history.append(st.session_state.messages[i])
    
    messages = []

    # 会話履歴付き
    for entry in chat_history:
        messages.append({
            'role': entry['role'],
            'content': entry['content']
        })

    # ユーザーから最後の質問付き
    messages.append({
        'role': 'user',
        'content': [
                {
                    "type": "text",
                    "text": question
                }
            ]
        })

    return messages

def build_messages(messages):
    return [
    {
        "role": entry["role"],
        "content": entry["content"]
    }
    for entry in messages
]

def send_message(prompt: str) -> dict:
    """Calls the REST API and returns the response."""
    request_body = {
        "messages": build_messages(prompt),
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{FILE}",
    }
    st.sidebar.write("request_body 出力")
    st.sidebar.write(request_body)
    
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )

def process_message(prompt: str) -> None:
    """Processes a message and adds the response to the chat."""
    st.session_state.messages.append(
        {"role": "user", "content": [{"type": "text", "text": prompt}]}
    )
    with st.chat_message("user"):
        st.markdown(prompt)
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            
            prompt_history = get_prompt_history(prompt)
            st.sidebar.write("prompt_history 出力")
            st.sidebar.write(prompt_history)
            
            response = send_message(prompt=prompt_history)           
            st.sidebar.write("response content 出力")
            st.sidebar.write(response)
            
            content = response["message"]["content"]
            display_content(content=content)
    st.session_state.messages.append({"role": "analyst", "content": content})

def display_content(content: list, message_index: int = None) -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):
                with st.spinner("Running SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()
                    st.dataframe(df)

st.title("Cortex analyst")

config_options()
init_messages()

for message_index, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        display_content(content=message["content"], message_index=message_index)

if user_input := st.chat_input("What is your question?"):
    process_message(prompt=user_input)

if st.session_state.active_suggestion:
    process_message(prompt=st.session_state.active_suggestion)
    st.session_state.active_suggestion = None