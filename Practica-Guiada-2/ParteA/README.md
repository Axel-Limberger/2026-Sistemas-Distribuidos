# Nodo IoT Smart Campus

Proyecto base para la parte A de la Practica Guiada 2: comunicacion MQTT + CoAP.

## Integrantes

- Ernst, Milagros Shaiel
- Limberger, Axel Agustín
- Verón, Juan Manuel

## Ejecucion

### Local

Requisito: broker MQTT corriendo en `localhost:1883` (puede ser NanoMQ local).

```bash
# Terminal 1: Broker (si no tienes uno local)
make broker-up

# Terminal 2: Nodo
make run
```

### Docker Compose (interactivo)

**1. Levantar solo el broker** (en background):
```bash
make broker-up
```

**2. Lanzar nodos** (en terminales separadas):
```bash
# Terminal 2: Nodo 1
make docker-nodo1

# Terminal 3: Nodo 2
make docker-nodo2
```

**3. Ver logs del broker o nodos**:
```bash
make docker-logs
```

**4. Detener todo**:
```bash
make broker-down
```

## Requisitos completados

- [x] Cliente MQTT con testamento (LWT): `nodo/{id}/estado` -> `{"estado":"offline"}`
- [x] Publicar estado `{"estado":"online"}` retenido tras conectar
- [x] Loop de lecturas simuladas cada 5 s en `campus/{edificio}/{aula}/sensor/temperatura` con QoS 1
- [x] Suscripcion a comandos en `campus/{edificio}/{aula}/actuador/cmd` con accion impresa
- [x] Servidor CoAP con recursos:
  - [x] `GET /temperatura` -> ultima lectura en JSON
  - [x] `PUT /config` -> actualizar configuracion local
  - [x] `GET /config` -> configuracion actual en JSON
- [x] Docker Compose con al menos 1 nodo + NanoMQ broker

## Captura de ejecucion

### **Captura 1**: broker NanoMQ corriendo en Docker.

![Broker NanoMQ](capturas/cap1.png)

### **Captura 2**: nodo iniciado, MQTT conectado, CoAP iniciado y lecturas publicándose.

![Nodo MQTT](capturas/cap2.png)

### **Captura 3**: comando MQTT recibido y acción simulada ejecutada.

![Comando MQTT](capturas/cap3.png)

### **Captura 4**: respuesta de GET /temperatura.

![CoAP Temperatura](capturas/cap4.png)

### **Captura 5**: respuesta de GET /config.

![CoAP config](capturas/cap5.png)

### **Captura 6**: PUT /config y luego GET /config mostrando "modo":"manual".

![Modo manual](capturas/cap6.png)

### **Captura 7**: estado online retenido.

![Estado online](capturas/cap7.png)

### **Captura 8**: estado offline publicado al cerrar el nodo.

![Estado offline](capturas/cap8.png)

### **Captura 9**: dos nodos Docker publicando lecturas.

![Docker nodos](capturas/cap9.png)
![Docker nodos](capturas/cap10.png)
