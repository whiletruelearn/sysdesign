
# Pre-requisites

- Install K8s
- Install kubectl
- Install aws-cli

# Setup localstack for local S3 

aws --endpoint-url=http://localhost:4566 s3 mb s3://bds-assignment
aws --endpoint-url=http://localhost:4566 s3 cp data_job_posts.csv s3://bds-assignment/data/data_job_post.csv
aws --endpoint-url=http://localhost:4566 s3 cp coursera_data.csv s3://bds-assignment/data/coursera_data.csv


# Deploy the kube objects and port-forward

kubectl create ns spark
kubectl apply -n spark -f jupyter.yaml
kubectl apply -n kube-system -f localstack

kubectl port-forward -n kube-system service/localstack 4566:4566
kubectl port-forward -n spark service/jupyter 8888:8888




# Jupyter Notebook

- Go to localhost:8888 and Run the cells in `BDS Assignment`

- Check `kubectl get pods -n spark` and you should see the worker pods coming up while getting the spark session.
- We can see the logs for the spark job by doing `kubectl logs` on the worker pods.
- If you see ImagePullbackError , try docker pull the base images used.
- Numpy is not there is the workers initially, patched this by SSH ing to the worker pods and installing manually. 
