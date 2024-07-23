FROM python:3.11-alpine

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . .

# Install dependencies do compile numpy and pandas
RUN apk --no-cache add musl-dev linux-headers g++

# Install dependencies specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Remove unnecessary libraries
RUN apk del musl-dev linux-headers g++

# Run the script when the container launches
CMD ["python", "update_checkouts.py"]