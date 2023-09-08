# Using an official Python runtime as a parent image
FROM python:3.9-slim

# Setting the working directory in the container
WORKDIR /app/trading_bot

# Copy the entire 'python_codes' folder (or just the 'trading_bot' subfolder) into the container
COPY python_codes/trading_bot/ /app/trading_bot/

# Installing necessary dependencies
RUN pip install pandas datetime pymongo FinMind python-dotenv

# Define default command (can be overridden at runtime)
CMD ["python", "crawl_stock_price.py"]
