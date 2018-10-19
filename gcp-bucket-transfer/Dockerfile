FROM gcr.io/google-appengine/python

# Copy the application's requirements.txt and run pip to install all
# dependencies into the virtualenv.
ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

# Add the application source code.
ADD bucket-transfer.py /app/bucket-transfer.py
RUN chmod +x /app/bucket-transfer.py

# Add app directory to path to call bucket-transfer from command-line
ENV PATH /app:$PATH

# Add Dockerfile to image
ADD Dockerfile /Dockerfile
