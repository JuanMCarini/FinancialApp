from pdf2image import convert_from_path
import pytesseract
import numpy as np
from ollama import Client
import re

client = Client()

def extract_text_from_pdf_ocr(pdf_path: str, lang: str = "spa") -> str:
    """
    Extracts text from each page of a PDF using OCR on rendered images.

    Parameters:
    - pdf_path (str): Path to the PDF file.
    - lang (str): Language code for OCR (default: 'spa' for Spanish).

    Returns:
    - str: Combined OCR text from all pages.
    """
    pages = convert_from_path(pdf_path, dpi=300)
    all_text = []

    for i, page in enumerate(pages):
        text = pytesseract.image_to_string(page, lang=lang)
        all_text.append(f"\n--- Página {i+1} ---\n{text}")

    return "\n".join(all_text)

def clean_pdf_text(text: str) -> str:
    """
    Clean up redundant and repetitive content from extracted OCR text.
    """
    text = re.sub(r'Documentos Adjuntos\n+', '', text)
    text = re.sub(r'\n{2,}', '\n', text)
    return text.strip()

def split_text(text: str, chunk_size: int = 1500, overlap: int = 100) -> list:
    """
    Splits text into overlapping chunks for better LLM context handling.
    """
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunks.append(text[start:end])
        start += chunk_size - overlap
    return chunks

def get_embedding(text: str) -> np.ndarray:
    """
    Generates an embedding vector for the given text using Mistral via Ollama.
    """
    response = client.embeddings(model='mistral', prompt=text)
    return np.array(response['embedding'])

def retrieve_relevant_chunk(question: str, chunks: list[str]) -> str:
    """
    Retrieves the most relevant text chunk based on semantic similarity.
    """
    question_vec = get_embedding(question)
    similarities = [np.dot(get_embedding(chunk), question_vec) for chunk in chunks]
    best_idx = int(np.argmax(similarities))
    return chunks[best_idx]

def ask_question_about_pdf(pdf_path: str, question: str, lang: str = "spa") -> str:
    """
    Extracts text from a scanned or complex PDF using OCR and asks a question to Mistral based on context.

    Parameters:
    - pdf_path (str): Path to the PDF.
    - question (str): Question about the document.
    - lang (str): OCR language code (default 'spa').

    Returns:
    - str: Mistral's answer.
    """
    raw_text = extract_text_from_pdf_ocr(pdf_path, lang)
    cleaned_text = clean_pdf_text(raw_text)
    chunks = split_text(cleaned_text)
    context = retrieve_relevant_chunk(question, chunks)

    response = client.chat(
        model="mistral",
        messages=[
            {"role": "system", "content": f"Contexto extraído del documento:\n{context}"},
            {"role": "user", "content": question}
        ]
    )
    return response['message']['content']