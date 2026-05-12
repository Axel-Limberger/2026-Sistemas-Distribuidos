package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	direccionServidor := os.Getenv("SERVIDOR")
	if direccionServidor == "" {
		direccionServidor = "localhost:4000"
	}

	nombre := os.Getenv("NOMBRE")
	if nombre == "" {
		fmt.Print("Ingrese su nombre: ")
		lector := bufio.NewReader(os.Stdin)
		nombreBytes, _, _ := lector.ReadLine()
		nombre = string(nombreBytes)
	}

	// TODO 20: conectar al servidor usando net.Dial("tcp", direccionServidor)
	// Manejar errores y usar defer conexion.Close()

	// TODO 21: enviar mensaje de identificación con protocolo.Codificar
	// mensaje de tipo "identificacion" con Emisor = nombre

	// TODO 22: iniciar una goroutine que escuche mensajes del servidor en paralelo
	// La goroutine debe usar protocolo.Decodificar en un bucle e imprimir los mensajes recibidos
	// Si hay error, imprimir y retornar (el servidor cerró la conexión)

	// TODO 23: en el hilo principal, leer líneas de stdin y enviar mensajes de tipo "broadcast"
	// Usar bufio.NewReader(os.Stdin) y ReadString('\n')
	// Para cada línea, crear un Mensaje y enviarlo con protocolo.Codificar

	log.Println("Cliente finalizado")
}

// recibirMensajes lee continuamente desde la conexión e imprime en consola
func recibirMensajes(conexion net.Conn) {
	// TODO 24: implementar bucle infinito de protocolo.Decodificar
	// Imprimir Emisor, Contenido y Timestamp de cada mensaje recibido
	// Si Decode retorna error, imprimir "Desconectado del servidor" y retornar
}
