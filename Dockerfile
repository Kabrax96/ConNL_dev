FROM public.ecr.aws/lambda/python:3.12
WORKDIR /var/task
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/var/task
CMD ["lambda_handler.handler"]
