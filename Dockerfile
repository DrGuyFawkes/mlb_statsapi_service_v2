FROM lambci/lambda:build-python3.7

COPY . .

ENTRYPOINT ["bash", "deploy.sh"]
CMD