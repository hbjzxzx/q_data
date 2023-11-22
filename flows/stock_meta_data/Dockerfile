FROM prefecthq/prefect:2.14-python3.11-conda

COPY . .
RUN pip install -r requirements.txt 

ENV PREFECT_API_URL=http://q_data-prefect-1:4200/api
# Run our flow script when the container starts
CMD ["python", "stock_list.py"]