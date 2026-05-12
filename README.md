# Servidor de Broadcast Concurrente

Proyecto base para la Clase sobre Sockets de Sistemas Distribuidos.

## Integrantes

- Apellidos y Nombres 1
- Apellidos y Nombres 2
- Apellidos y Nombres 3

## Ejecución

### Local

```bash
# Terminal 1: servidor
go run ./cmd/servidor

# Terminal 2: cliente
go run ./cmd/cliente
```

### Docker Compose

```bash
docker-compose up --build
```

## Requisitos completados

- [ ] Servidor TCP concurrente
- [ ] Protocolo JSON
- [ ] Registro de clientes con sync.RWMutex
- [ ] Broadcast a todos los clientes
- [ ] Cliente interactivo (stdin + recepción paralela)
- [ ] Docker + docker-compose
- [ ] Bonus: descubrimiento UDP

## Captura de ejecución

(Adjuntar log o captura de pantalla con múltiples clientes conectados)
