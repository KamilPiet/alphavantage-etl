FROM public.ecr.aws/lambda/python:3.9

# Install the function's dependencies using file requirements.txt
# from your project folder.

COPY aws-lambda/requirements.txt  .
RUN  pip3 install -r requirements.txt

# Copy function code
COPY av_etl.py ${LAMBDA_TASK_ROOT}
COPY aws-lambda/app.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "app.handler" ]