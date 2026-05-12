package main

import (
	"log"
	"net"
	"os"

	"sd-broadcast/internal/registro"
	"sd-broadcast/pkg/protocolo"
)

const puertoPorDefecto = "4000"

func main() {
	puerto := os.Getenv("PUERTO")
	if puerto == "" {
		puerto = puertoPorDefecto
	}

	escuchador, err := net.Listen("tcp", ":"+puerto)
	if err != nil {
		log.Fatalf("No se pudo iniciar el escuchador: %v", err)
	}
	defer escuchador.Close()

	log.Printf("Servidor de broadcast escuchando en :%s", puerto)

	// TODO 8: crear un RegistroClientes usando registro.NuevoRegistro()
	registroClientes := registro.NuevoRegistro()

	// TODO 9: iniciar goroutine para descubrimiento UDP (bonus)
	// go iniciarDescubrimientoUDP(puerto)
	go iniciarDescubrimientoUDP(puerto)

	for {
		conexion, err := escuchador.Accept()
		if err != nil {
			log.Printf("Error al aceptar conexión: %v", err)
			continue
		}

		// TODO 10: en lugar de llamar directamente a manejarCliente,
		// lanzar una goroutine para atender la conexión concurrentemente
		go manejarCliente(conexion, registroClientes)
	}
}

func manejarCliente(conexion net.Conn, registroClientes *registro.RegistroClientes) {
	defer conexion.Close()

	// TODO 11: leer el primer mensaje de identificación del cliente
	// Usar protocolo.Decodificar para obtener el nombre del emisor
	mensaje, err := protocolo.Decodificar(conexion)

	if err != nil {
		log.Printf("Error al decodificar mensaje de identificacion: %v", err)
		return
	}

	// Usar el mensaje decodificado para obtener el nombre del cliente
	nombreCliente := mensaje.Emisor

	log.Printf("Cliente conectado: %s desde %s", nombreCliente, conexion.RemoteAddr())

	// TODO 12: agregar el cliente al registro usando registroClientes.Agregar(nombreCliente, conexion)
	registroClientes.Agregar(nombreCliente, conexion)

	// TODO 13: notificar a todos los demás clientes que "nombreCliente se unió"
	// Usar difundirMensaje excepto al emisor
	mensajeUnion := protocolo.NuevoMensaje("Sistema", nombreCliente+" se unio", "sistema")
	difundirMensaje(registroClientes, mensajeUnion, nombreCliente)

	// TODO 14: defer para eliminar al cliente del registro al desconectar
	// defer registroClientes.Eliminar(nombreCliente)
	// defer difundirMensaje(registroClientes, protocolo.NuevoMensaje("Sistema", nombreCliente+" se desconectó", "sistema"), nombreCliente)
	defer func() {
		registroClientes.Eliminar(nombreCliente)
		mensajeDesconexion := protocolo.NuevoMensaje("Sistema", nombreCliente+" se desconecto", "sistema")
		difundirMensaje(registroClientes, mensajeDesconexion, nombreCliente)
	}()

	// TODO 15: bucle para leer mensajes del cliente y reenviarlos a todos los demás
	// Usar protocolo.Decodificar en un for {}
	// Si el mensaje.Tipo es "broadcast", usar difundirMensaje
	// Si hay error en Decode, salir del bucle (cliente desconectado)

	for {
		mensaje, err := protocolo.Decodificar(conexion)

		if err != nil {
			log.Printf("Error al decodificar mensaje de %s: %v", nombreCliente, err)
			break
		}

		if mensaje.Tipo == "broadcast" {
			difundirMensaje(registroClientes, mensaje, nombreCliente)
		}
	}
}

// difundirMensaje envía un mensaje a todos los clientes excepto al emisor indicado
func difundirMensaje(registroClientes *registro.RegistroClientes, mensaje protocolo.Mensaje, exceptoEmisor string) {
	// TODO 16: obtener todas las conexiones del registro
	clientes := registroClientes.ObtenerClientes()

	// TODO 17: iterar sobre las conexiones
	for nombre, conexion := range clientes {

		// TODO 18: si el emisor de esa conexión no es exceptoEmisor, enviar el mensaje con protocolo.Codificar
		if nombre == exceptoEmisor {
			continue
		}
		err := protocolo.Codificar(conexion, mensaje)

		// TODO 19: si Codificar retorna error, ignorar (el cliente puede haberse desconectado abruptamente)
		if err != nil {
			log.Printf("Error al enviar mensaje a %s: %v", nombre, err)
		}
	}
}

// iniciarDescubrimientoUDP es una función ficticia para manejar el descubrimiento UDP
func iniciarDescubrimientoUDP(puerto string) {
	log.Printf("Descubrimiento UDP iniciado en el puerto %s", puerto)
}
