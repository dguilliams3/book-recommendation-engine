FROM python:3.11-slim
WORKDIR /app
COPY ../../src/common /app/common
COPY . /app/streamlit_ui
RUN pip install --no-cache-dir poetry && \
    poetry config virtualenvs.create false && \
    poetry init -n --python "^3.11" && \
    poetry add streamlit requests
EXPOSE 8501
CMD ["streamlit", "run", "streamlit_ui/app.py", "--server.port", "8501", "--server.address", "0.0.0.0"] 