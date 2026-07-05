package gossip

import (
	"fmt"
	"log"
	"os"
)

// IniciarModuloGossip abstrae la configuración inicial del protocolo de chisme.
// Recibe los datos esenciales del nodo y valida que el entorno de red sea consistente.
func IniciarModuloGossip(id int, direccionRPC string, seedAddr string) *NodoGossip {
	log.Printf("[GOSSIP-INIT] Inicializando nodo lógico ID %d en la dirección %s", id, direccionRPC)
	
	// Invocamos al constructor que completamos en la entrega anterior
	instanciaNodo := NuevoNodo(id, direccionRPC)

	// Si en el archivo docker-compose.yml o las variables locales definimos un nodo SEED,
	// procedemos a engancharnos a la red existente de forma asíncrona.
	if seedAddr != "" {
		log.Printf("[GOSSIP-INIT] Nodo %d intentando acoplarse al nodo semilla (SEED): %s", id, seedAddr)
		instanciaNodo.Unirse(seedAddr)
	} else {
		log.Printf("[GOSSIP-INIT] Nodo %d lanzado como nodo Raíz/Semilla (No posee SEED externo)", id)
	}

	return instanciaNodo
}

// ObtenerHostnameLocal es una función auxiliar útil para entornos Docker.
// Evita duplicados molestos resolviendo dinámicamente si el nodo se identifica por IP o Hostname.
func ObtenerHostnameLocal(puerto string) string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	return fmt.Sprintf("%s:%s", hostname, puerto)
}