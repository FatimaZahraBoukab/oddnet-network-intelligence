FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir streamlit pandas sqlalchemy psycopg2-binary plotly streamlit-option-menu

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.address=0.0.0.0"]
