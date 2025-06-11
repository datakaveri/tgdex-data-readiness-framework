# Use an official Python runtime as a parent image
FROM public.ecr.aws/lambda/python:3.10

# Copy requirements.txt
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of code
COPY . .

# Set the entrypoint
CMD ["lambda_handler.lambda_handler"]  

