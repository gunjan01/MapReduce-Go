echo "Create a K8s cluster"
kind create cluster

echo "Create namespace"
kubectl create ns myspace
kubectl config set-context --current --namespace=myspace

#echo "Loading and deploy the etcd distributed db"
#helm repo add bitnami https://charts.bitnami.com/bitnami
#helm install -n myspace etcd bitnami/etcd --set auth.rbac.enabled=false

#echo "Load up docker images"
#kind load docker-image master
#kind load docker-image worker

echo "Creating 2 master and one worker pods"
kubectl -n myspace apply -f deployment-etcd.yml
kubectl -n myspace apply -f deployment-worker.yml
kubectl -n myspace apply -f deployment-master.yml

echo "Checking deployment status"
kubectl get all -n myspace
kubectl -n myspace logs pod/<your-pod-id>
kubectl -n myspace delete pod/<your-pod-id>

