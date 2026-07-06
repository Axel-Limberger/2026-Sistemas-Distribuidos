package replicacion

import (
	"log"
	"net/rpc"
	"sync"
)

// Estructuras de mensajes RPC provistas por la cátedra[cite: 962].
type ArgsEscritura struct {
	Clave     string
	Valor     string
	Timestamp int64
}
type RespEscritura struct {
	Exito  bool
	NodoID string
}
type ArgsLectura struct {
	Clave string
}
type RespLectura struct {
	Valor      string
	Timestamp  int64
	NodoID     string
	Encontrado bool
}

// TODO 1: Definir QuorumConfig con N, W, R[cite: 963].
type QuorumConfig struct {
	N int
	W int
	R int
}

// Validar retorna W+R > N para asegurar la intersección de quórums[cite: 963].
func (qc QuorumConfig) Validar() bool {
	return qc.W+qc.R > qc.N
}

// Registro guarda el valor y cuándo fue escrito (Last-Write-Wins).
type Registro struct {
	Valor     string
	Timestamp int64
}

// Todo 2: Store es el almacenamiento local con timestamps protegido por un RWMutex[cite: 963, 964].
type Store struct {
	mu    sync.RWMutex
	datos map[string]Registro
}

// TODO 3: Implementar NuevoStore[cite: 964, 965].
func NuevoStore() *Store {
	return &Store{
		datos: make(map[string]Registro),
	}
}

// TODO 4: Implementar Escribir[cite: 965, 966, 967].
// Si el timestamp recibido es mayor o igual al almacenado, actualizar.
func (s *Store) Escribir(clave, valor string, timestamp int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	reg, existe := s.datos[clave]
	if !existe || timestamp >= reg.Timestamp {
		s.datos[clave] = Registro{
			Valor:     valor,
			Timestamp: timestamp,
		}
		return true
	}
	return false
}

// TODO 5: Implementar Leer[cite: 968, 969].
// Retornar valor, timestamp y un bool indicando si la clave existe.
func (s *Store) Leer(clave string) (string, int64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	reg, existe := s.datos[clave]
	if !existe {
		return "", 0, false
	}
	return reg.Valor, reg.Timestamp, true
}

// TODO 6: Implementar Sincronizar (idempotente para read-repair)[cite: 969, 970, 971].
func (s *Store) Sincronizar(clave, valor string, timestamp int64) bool {
	return s.Escribir(clave, valor, timestamp)
}

// ServicioQuorum expone métodos RPC para lecturas y escrituras con quórum[cite: 971, 972].
type ServicioQuorum struct {
	NodoID string
	Store  *Store
	Pares  []string
	Config QuorumConfig
}

// TODO 7: Implementar Escribir (RPC Servidor)[cite: 973, 974].
func (s *ServicioQuorum) Escribir(args ArgsEscritura, resp *RespEscritura) error {
	resp.Exito = s.Store.Escribir(args.Clave, args.Valor, args.Timestamp)
	resp.NodoID = s.NodoID
	return nil
}

// TODO 8: Implementar Leer (RPC Servidor)[cite: 974, 975, 976].
func (s *ServicioQuorum) Leer(args ArgsLectura, resp *RespLectura) error {
	valor, ts, encontrado := s.Store.Leer(args.Clave)
	resp.Valor = valor
	resp.Timestamp = ts
	resp.NodoID = s.NodoID
	resp.Encontrado = encontrado
	return nil
}

// TODO 9: Implementar Sincronizar (RPC Servidor para read-repair)[cite: 976, 977].
func (s *ServicioQuorum) Sincronizar(args ArgsEscritura, resp *RespEscritura) error {
	resp.Exito = s.Store.Sincronizar(args.Clave, args.Valor, args.Timestamp)
	resp.NodoID = s.NodoID
	if resp.Exito {
		log.Printf("[STORE %s] Read-repair %s=%s (ts=%d)", s.NodoID, args.Clave, args.Valor, args.Timestamp)
	}
	return nil
}

// TODO 10: Implementar CoordinarEscritura (Cliente RPC)[cite: 978, 979, 980].
// Conecta RPC a cada par, invoca Escribir y retorna true si W o más confirmaron (contando al coordinador).
func CoordinarEscritura(clave, valor string, timestamp int64, pares []string, w int) bool {
	votos := 1 // Contamos 1 voto inicial (el del propio coordinador que escribe localmente)
	var mu sync.Mutex
	var wg sync.WaitGroup

	args := ArgsEscritura{Clave: clave, Valor: valor, Timestamp: timestamp}

	for _, parAddr := range pares {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var resp RespEscritura
			err = client.Call("ServicioQuorum.Escribir", args, &resp)
			if err == nil && resp.Exito {
				mu.Lock()
				votos++
				mu.Unlock()
			}
		}(parAddr)
	}

	wg.Wait()
	return votos >= w
}

// TODO 11: Implementar CoordinarLectura (Cliente RPC)[cite: 980, 981, 982, 983].
// Devuelve el valor con el timestamp más grande y true si obtuvo al menos R respuestas (incluida la local).
func CoordinarLectura(clave string, pares []string, r int) (string, int64, bool) {
	type respuestaConAddr struct {
		addr string
		resp RespLectura
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var respuestas []respuestaConAddr

	args := ArgsLectura{Clave: clave}

	for _, parAddr := range pares {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
			}
			defer client.Close()

			var resp RespLectura
			err = client.Call("ServicioQuorum.Leer", args, &resp)
			if err == nil {
				mu.Lock()
				respuestas = append(respuestas, respuestaConAddr{addr: addr, resp: resp})
				mu.Unlock()
			}
		}(parAddr)
	}

	wg.Wait()

	if len(respuestas) < r {
		return "", 0, false
	}

	var masReciente RespLectura
	encontradoAlguno := false

	for _, rc := range respuestas {
		if rc.resp.Encontrado {
			if !encontradoAlguno || rc.resp.Timestamp > masReciente.Timestamp {
				masReciente = rc.resp
				encontradoAlguno = true
			}
		}
	}

	// Read-Repair asíncrono: si algún nodo posee una versión desactualizada, enviamos Sincronizar
	if encontradoAlguno {
		for _, rc := range respuestas {
			if rc.resp.Timestamp < masReciente.Timestamp {
				go func(addr string) {
					client, err := rpc.Dial("tcp", addr)
					if err != nil {
						return
					}
					defer client.Close()

					argsRepair := ArgsEscritura{
						Clave:     clave,
						Valor:     masReciente.Valor,
						Timestamp: masReciente.Timestamp,
					}
					var respRepair RespEscritura
					client.Call("ServicioQuorum.Sincronizar", argsRepair, &respRepair)
				}(rc.addr)
			}
		}
		return masReciente.Valor, masReciente.Timestamp, true
	}

	return "", 0, true // La clave no existe en ninguna réplica
}
