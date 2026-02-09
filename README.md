### Desde la carpeta api/:

`
docker build -t productos-api:1.1.0 .
docker images | grep productos-api
`

### Despliegue de Manifiesto
`
kubectl apply -f k8s-productos-api-nodeport.yaml
`

### Validacion del despliegue:

`
  kubectl -n api get pods -o wide
  kubectl -n api get svc productos-api -o wide
  kubectl -n api describe pod -l app=productos-api | tail -n 40
  kubectl -n api logs -l app=productos-api --tail=100
`

### Validar el acceso
`
  http://100.72.137.93:8090/docs
`