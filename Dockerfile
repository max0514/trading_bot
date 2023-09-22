# Using an official Python runtime as a parent image
FROM python:3.9-slim

# Setting the working directory in the container
WORKDIR /app/trading_bot

# Copy the entire  'trading_bot' folder into the container
COPY company_data.csv mongo.py .env   /app/trading_bot/


# Installing necessary dependencies
RUN pip install pandas datetime pymongo FinMind python-dotenv 

# Define default command (can be overridden at runtime)
CMD ["python", "stock_price.py"]
