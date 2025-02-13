import json
import os
import sys
import boto3
import botocore.config
import streamlit as st

# Will be using Titan Embeddings Model to generate Embedding

# We need Lang Chain for creating embeddings, it will provide multiple ways to create embeddings and interact with AWS.

from langchain_aws.embeddings import BedrockEmbeddings
from langchain_aws import BedrockLLM

# Data Ingestion Rqmts

import numpy as np
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFDirectoryLoader

# Vector Embedding and Vector Store

from langchain_community.vectorstores import FAISS

# LLM Models & Responses

from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA

# Bedrock Clients
bedrock = boto3.client(service_name="bedrock-runtime", region_name="ap-southeast-2", config=botocore.config.Config(
    read_timeout=300,
    retries={
        'max_attempts': 3
    }
))
bedrock_embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-image-v1", client=bedrock)


# Data Ingestion

def data_ingestion():
    loader = PyPDFDirectoryLoader("rag_app_document_qa/data")
    documents = loader.load()

    # - in our testing character split works better with this PDF Data set
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=1000)
    docs = text_splitter.split_documents(documents)
    return docs


# Vector Embeddings and Vector Store

FAISS_INDEX_PATH = "rag_app_document_qa/faiss_index"


def create_vector_store(docs):
    vector_store_faiss = FAISS.from_documents(docs, bedrock_embeddings)
    vector_store_faiss.save_local(FAISS_INDEX_PATH)


# Work with LL M's

def get_claude_llm():
    # Create Anthropic Model
    llm = BedrockLLM(model_id="anthropic.claude-3-sonnet-20240229-v1:0", client=bedrock, model_kwargs={
        "max_tokens": 512,
    })
    return llm


def get_mistral_llm():
    # Create Mistral Model
    llm = BedrockLLM(model_id="mistral.mistral-large-2402-v1:0", client=bedrock, model_kwargs={
        "max_tokens": 512,
    })
    return llm


def get_aws_titan_llm():
    llm = BedrockLLM(model_id="amazon.titan-text-express-v1", client=bedrock, model_kwargs={
        "maxTokenCount": 512,
    })
    return llm


prompt_template = """Human: Use the following pieces of context to provide a concise answer to the question at the 
end but use at least well-summarized 250 words with proper and detailed explanations. If you don't know the answer, 
just say that you don't know, don't try to make up an answer. 

<context> {context} </context>

Question: {question}

Assistant:
"""

PROMPT = PromptTemplate(
    template=prompt_template, input_variables=["context", "question"]
)


def get_response_llm(llm, vectorstore_faiss, query):
    qa = RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=vectorstore_faiss.as_retriever(
            search_type="similarity", search_kwargs={"k": 3}
        ),
        return_source_documents=True,
        chain_type_kwargs={"prompt": PROMPT},
    )

    answer = qa.invoke({"query": query})
    return answer['result']


def main():
    st.set_page_config("Chat PDF")
    st.header("Chat with PDF using AWS Bedrock 🤖")

    user_question = st.text_input("Enter your question")

    with st.sidebar:
        st.title("Update or Create Vector Store:")

        if st.button("Vectors Update"):
            with st.spinner("Processing ..."):
                docs = data_ingestion()
                create_vector_store(docs)
                st.success("Done!")

    if st.button("Mistral LLM Output"):
        with st.spinner("Processing ..."):
            faiss_index = FAISS.load_local(FAISS_INDEX_PATH, bedrock_embeddings, allow_dangerous_deserialization=True)
            llm = get_mistral_llm()

            st.write(get_response_llm(llm, faiss_index, user_question))
            st.success("Done!")

    if st.button("Amazon Titan LLM Output"):
        with st.spinner("Processing ..."):
            faiss_index = FAISS.load_local(FAISS_INDEX_PATH, bedrock_embeddings, allow_dangerous_deserialization=True)
            llm = get_aws_titan_llm()

            st.write(get_response_llm(llm, faiss_index, user_question))
            st.success("Done!")


if __name__ == "__main__":
    main()
