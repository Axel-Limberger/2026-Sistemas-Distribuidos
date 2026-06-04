package detector

// TODO 5-8: Implementar envio y recepcion de heartbeats UDP.
// Necesitaras importar:
//   "encoding/json"
//   "fmt"
//   "net"
//   "time"
//   "sd-comunicacion/pkg/protocolo"

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"sd-comunicacion/pkg/protocolo"
)

// Enviador se encarga de enviar heartbeats UDP periodicamente
type Enviador struct {
	destino   string
	intervalo time.Duration // TODO: usar time.Duration en vez de int64
	nodoID    string
	contador  int
}

// TODO 5: Implementar la funcion NuevaEnviador.
// Debe recibir destino (string), intervalo (time.Duration) y nodoID (string).

func NuevaEnviador(destino string, intervalo time.Duration, nodoID string) *Enviador {
	return &Enviador{
		destino:   destino,
		intervalo: intervalo,
		nodoID:    nodoID,
	}
}

// TODO 6: Implementar el metodo (e *Enviador) Iniciar().
// Debe enviar Heartbeat cada 'intervalo' por UDP al destino configurado.

func (e *Enviador) Iniciar() {
	addr, err := net.ResolveUDPAddr("udp", e.destino)
	if err != nil {
		fmt.Printf("[HEARTBEAT] Error resolviendo destino %s: %v\n", e.destino, err)
		return
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		fmt.Printf("[HEARTBEAT] Error conectando UDP: %v\n", err)
		return
	}
	defer conn.Close()

	ticker := time.NewTicker(e.intervalo)
	defer ticker.Stop()

	for range ticker.C {
		e.contador++

		hb := protocolo.Heartbeat{
			NodoID:    e.nodoID,
			Timestamp: time.Now().Unix(),
			Contador:  e.contador,
		}

		data, err := json.Marshal(hb)
		if err != nil {
			fmt.Printf("[HEARTBEAT] Error serializando heartbeat: %v\n", err)
			continue
		}

		if _, err := conn.Write(data); err != nil {
			fmt.Printf("[HEARTBEAT] Error enviando heartbeat: %v\n", err)
			continue
		}

		fmt.Printf("[HEARTBEAT] Enviado a %s | contador=%d\n", e.destino, e.contador)
	}
}

// Receptor escucha heartbeats y detecta si dejan de llegar.
// Debe manejar estados: alive -> suspect -> dead.
type Receptor struct {
	puerto  string
	timeout time.Duration // TODO: usar time.Duration en vez de int64
	// ultimo debe guardar time.Time o timestamp del ultimo heartbeat recibido
	ultimo time.Time
	// estado puede ser "alive", "suspect" o "dead"
	estado string
	mu     sync.Mutex
}

// TODO 7: Implementar la funcion NuevoReceptor.
// Debe recibir puerto (string) y timeout (time.Duration).
func NuevoReceptor(puerto string, timeout time.Duration) *Receptor {
	return &Receptor{
		puerto:  puerto,
		timeout: timeout,
		ultimo:  time.Now(),
		estado:  "suspect",
	}
}

// TODO 8: Implementar el metodo (r *Receptor) Escuchar().
// Debe:
//   - Escuchar UDP en 'puerto'
//   - Decodificar mensajes JSON tipo protocolo.Heartbeat
//   - Actualizar ultimo timestamp al recibir
//   - En una goroutine separada, revisar periodicamente:
//       si time.Since(ultimo) > timeout: pasar a "suspect"
//       (opcional) si time.Since(ultimo) > 2*timeout: pasar a "dead"
//   - Imprimir cambios de estado por consola

// Escuchar recibe heartbeats UDP y actualiza el estado del servidor.
func (r *Receptor) Escuchar() {
	addr, err := net.ResolveUDPAddr("udp", r.puerto)
	if err != nil {
		fmt.Printf("[DETECTOR] Error resolviendo puerto %s: %v\n", r.puerto, err)
		return
	}

	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("[DETECTOR] Error escuchando UDP en %s: %v\n", r.puerto, err)
		return
	}
	defer conn.Close()

	fmt.Printf("[DETECTOR] Escuchando heartbeats en %s\n", r.puerto)

	go r.monitorear()

	buffer := make([]byte, 1024)

	for {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			fmt.Printf("[DETECTOR] Error leyendo UDP: %v\n", err)
			continue
		}

		var hb protocolo.Heartbeat
		if err := json.Unmarshal(buffer[:n], &hb); err != nil {
			fmt.Printf("[DETECTOR] Heartbeat inválido desde %s: %v\n", remoteAddr, err)
			continue
		}

		r.mu.Lock()
		r.ultimo = time.Now()

		if r.estado != "alive" {
			fmt.Printf("[DETECTOR] Estado cambiado: %s -> alive\n", r.estado)
			r.estado = "alive"
		}

		r.mu.Unlock()

		fmt.Printf(
			"[DETECTOR] Heartbeat recibido de %s | contador=%d | origen=%s\n",
			hb.NodoID,
			hb.Contador,
			remoteAddr.String(),
		)
	}
}

func (r *Receptor) monitorear() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		r.mu.Lock()

		transcurrido := time.Since(r.ultimo)
		nuevoEstado := r.estado

		if transcurrido > 2*r.timeout {
			nuevoEstado = "dead"
		} else if transcurrido > r.timeout {
			nuevoEstado = "suspect"
		} else {
			nuevoEstado = "alive"
		}

		if nuevoEstado != r.estado {
			fmt.Printf("[DETECTOR] Estado cambiado: %s -> %s\n", r.estado, nuevoEstado)
			r.estado = nuevoEstado
		}

		r.mu.Unlock()
	}
}
