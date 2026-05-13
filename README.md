# Servidor de Broadcast Concurrente

Proyecto base para la Clase sobre Sockets de Sistemas Distribuidos.

## Integrantes

- Ernst, Milagros Shaiel
- Limberger, Axel Agustín
- Verón, Juan Manuel

## Descripción

El proyecto implementa un servidor TCP concurrente capaz de recibir conexiones de múltiples clientes simultáneamente.  
Los clientes envían mensajes en formato JSON al servidor, y el servidor los reenvía al resto de los clientes conectados mediante un mecanismo de broadcast.

El servidor administra las conexiones concurrentes evitando condiciones de carrera mediante el uso de sincronización.

## Instrucciones para ejecutar

### Ejecución local

Para ejecutar el servidor, abrir una terminal y correr:

```bash
go run ./cmd/servidor
```

Para ejecutar un cliente, abrir otra terminal y correr:

```bash
go run ./cmd/cliente
```

### Docker Compose

Para ejecutar el proyecto con Docker Compose:

```bash
docker-compose up --build
```

Para detener la ejecución:

```bash
docker-compose down
```

## Log de ejecución
