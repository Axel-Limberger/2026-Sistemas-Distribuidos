package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"

	"sd-datastore/pkg/gossip"
	"sd-datastore/pkg/replicacion"
)

var (
	idNodo     string
	puertoHTTP string
	puertoRPC  string
	pares      []string
	gossipNodo *gossip.NodoGossip
	servicioQ  *replicacion.ServicioQuorum
	configQ    replicacion.QuorumConfig
	storeLocal *replicacion.Store
)

func main() {
	idNodo = os.Getenv("NODO_ID")
	if idNodo == "" {
		idNodo = "1"
	}
	puertoHTTP = os.Getenv("HTTP_PORT")
	if puertoHTTP == "" {
		puertoHTTP = "8080"
	}
	puertoRPC = os.Getenv("RPC_PORT")
	if puertoRPC == "" {
		puertoRPC = "5000"
	}

	pares = parsearPares(os.Getenv("PEERS"))

	// TODO 12: Parsear QUORUM_N, QUORUM_W, QUORUM_R de las variables de entorno.
	n, _ := strconv.Atoi(obtenerEnv("QUORUM_N", "3"))
	w, _ := strconv.Atoi(obtenerEnv("QUORUM_W", "2"))
	r, _ := strconv.Atoi(obtenerEnv("QUORUM_R", "2"))
	configQ = replicacion.QuorumConfig{N: n, W: w, R: r}
	if !configQ.Validar() {
		log.Fatal("Configuración de quórum inválida: debe cumplirse W + R > N")
	}

	idNum, _ := strconv.Atoi(idNodo)
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "localhost"
	}
	miDireccionRPC := fmt.Sprintf("%s:%s", hostname, puertoRPC)

	// Inicializar gossip
	gossipNodo = gossip.NuevoNodo(idNum, miDireccionRPC)

	seed := os.Getenv("SEED")
	if seed != "" {
		gossipNodo.Unirse(seed)
	}

	// TODO 13: Inicializar Store, ServicioQuorum y QuorumConfig.
	storeLocal = replicacion.NuevoStore()
	servicioQ = &replicacion.ServicioQuorum{
		NodoID: idNodo,
		Store:  storeLocal,
		Pares:  pares,
		Config: configQ,
	}

	// Endpoints HTTP
	http.HandleFunc("/estado", manejadorEstado)
	http.HandleFunc("/datos/", manejadorDatos)

	// Servicio RPC
	go iniciarRPC()

	// Loop anti-entropia
	go bucleAntiEntropia()

	addr := ":" + puertoHTTP
	fmt.Printf("[NODO %s] Escuchando HTTP en %s, RPC en %s\n", idNodo, addr, puertoRPC)
	log.Fatal(http.ListenAndServe(addr, nil))
}

// TODO 12b: Implementar parsearPares.
// Convierte "2=nodo2:5000,3=nodo3:5000" en []string{"nodo2:5000", "nodo3:5000"}
func parsearPares(peersEnv string) []string {
	if peersEnv == "" {
		return []string{}
	}
	lista := strings.Split(peersEnv, ",")
	resultado := make([]string, 0, len(lista))
	for _, p := range lista {
		partes := strings.Split(p, "=")
		if len(partes) == 2 {
			resultado = append(resultado, partes[1])
		}
	}
	return resultado
}

// TODO 14: Implementar iniciarRPC.
// Crear listener TCP, registrar ServicioGossip y ServicioQuorum, atender conexiones.
func iniciarRPC() {
	server := rpc.NewServer()

	// Registramos el servicio Quorum construido en la Parte 1
	if err := server.Register(servicioQ); err != nil {
		log.Fatalf("Error al registrar ServicioQuorum: %v", err)
	}

	// Registramos el envoltorio del servicio Gossip provisto por la cátedra
	servGossip := &gossip.ServicioGossip{Nodo: gossipNodo}
	if err := server.Register(servGossip); err != nil {
		log.Fatalf("Error al registrar ServicioGossip: %v", err)
	}

	listener, err := net.Listen("tcp", ":"+puertoRPC)
	if err != nil {
		log.Fatalf("Error levantando listener RPC: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go server.ServeConn(conn)
	}
}

// TODO 15: Implementar bucleAntiEntropia.
// Cada 5 segundos obtener un par con gossipNodo.AntiEntropia(), conectarse via RPC, intercambiar miembros y fusionar
func bucleAntiEntropia() {
	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		parAddr := gossipNodo.AntiEntropia()
		if parAddr == "" {
			continue
		}

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			// Estructuramos la petición Push-Pull usando los tipos exactos de tu archivo
			req := gossip.Intercambio{
				Remitente: gossipNodo.Direccion,
				Miembros:  gossipNodo.ObtenerMiembros(),
			}
			var resp gossip.Intercambio

			// Invocamos el método expuesto por el par remoto
			err = client.Call("ServicioGossip.Intercambiar", req, &resp)
			if err == nil {
				gossipNodo.FusionarMiembros(resp.Miembros)
				gossipNodo.Unirse(resp.Remitente)
			}
		}(parAddr)
	}
}

func manejadorEstado(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"node_id":  idNodo,
		"miembros": gossipNodo.ObtenerMiembros(),
		"quorum": map[string]int{
			"N": configQ.N,
			"W": configQ.W,
			"R": configQ.R,
		},
		"pares": pares,
	})
}

// manejadorDatos maneja PUT /datos/{clave} y GET /datos/{clave} sincronizando la réplica local.
func manejadorDatos(w http.ResponseWriter, r *http.Request) {
	partes := strings.Split(strings.TrimPrefix(r.URL.Path, "/datos/"), "/")
	if len(partes) == 0 || partes[0] == "" {
		http.Error(w, "falta clave", http.StatusBadRequest)
		return
	}

	clave := partes[0]

	// Incluimos de forma implícita nuestra propia dirección para complementar el quórum
	todosLosPares := append(pares, fmt.Sprintf("localhost:%s", puertoRPC))

	switch r.Method {
	case http.MethodPut:
		var body struct {
			Valor string `json:"valor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		timestamp := time.Now().UnixNano()
		exitoQuorum := replicacion.CoordinarEscritura(clave, body.Valor, timestamp, pares, configQ.W)

		if !exitoQuorum {
			http.Error(w, "503 Service Unavailable: Quórum de escritura no alcanzado", http.StatusServiceUnavailable)
			return
		}

		// Escribimos localmente también
		storeLocal.Escribir(clave, body.Valor, timestamp)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "OK", "clave": clave, "valor": body.Valor})

	case http.MethodGet:
		// Consultamos la red
		valor, _, ok := replicacion.CoordinarLectura(clave, todosLosPares, configQ.R)
		if !ok {
			// Fallback: si la red falla temporalmente o se particiona, intentamos responder con nuestra lectura local
			if valLoc, _, encLoc := storeLocal.Leer(clave); encLoc {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]string{"clave": clave, "valor": valLoc, "nota": "lectura local síncrona sin quórum estricto"})
				return
			}
			http.Error(w, "503 Service Unavailable: Quórum de lectura no alcanzado", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"clave": clave, "valor": valor})

	default:
		http.Error(w, "metodo no soportado", http.StatusMethodNotAllowed)
	}
}

func obtenerEnv(clave, valorPorDefecto string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return valorPorDefecto
}
