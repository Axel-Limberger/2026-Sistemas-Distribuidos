package telemetria

import (
	"fmt"
	"sync"
	"time"

	"sd-comunicacion/pkg/protocolo"
)

// TODO 1: Definir el struct Telemetria que sera el servicio RPC.
// Debe contener un mapa protegido por sync.Mutex para almacenar
// la ultima lectura de cada sensor.
// Sugerencia: usar map[string]Lectura
//
// import "sync" cuando lo necesites

type Telemetria struct {
	mu       sync.Mutex
	lecturas map[string]protocolo.Lectura
	contador int
}

func NuevoTelemetria() *Telemetria {
	return &Telemetria{
		lecturas: make(map[string]protocolo.Lectura),
	}
}

// TODO 2: Implementar el metodo RPC RegistrarLectura.
// Firma requerida por net/rpc:
//   func (t *Telemetria) RegistrarLectura(args Lectura, resp *RespuestaLectura) error
// Debe:
//   - Guardar la lectura en el mapa (protegiendo con mutex)
//   - Asignar un ID incremental a la respuesta
//   - Loguear la lectura recibida (import "fmt" y "time")
//   - Retornar nil en caso de exito

func (t *Telemetria) RegistrarLectura(args protocolo.Lectura, resp *protocolo.RespuestaLectura) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.contador++
	t.lecturas[args.SensorID] = args

	resp.ID = t.contador
	resp.Mensaje = "lectura registrada correctamente"

	fmt.Printf(
		"[RPC] Lectura recibida | id=%d sensor=%s temp=%.2f timestamp=%s\n",
		resp.ID,
		args.SensorID,
		args.Temperatura,
		time.Unix(args.Timestamp, 0).Format(time.RFC3339),
	)

	return nil
}

// TODO 3: Implementar el metodo RPC ObtenerUltimaLectura.
// Firma requerida por net/rpc:
//   func (t *Telemetria) ObtenerUltimaLectura(args ConsultaUltimaLectura, resp *Lectura) error
// Debe:
//   - Buscar en el mapa la ultima lectura del SensorID solicitado
//   - Si no existe, retornar un error con fmt.Errorf
//   - Si existe, copiar el valor a resp y retornar nil

func (t *Telemetria) ObtenerUltimaLectura(args protocolo.ConsultaUltimaLectura, resp *protocolo.Lectura) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	lectura, ok := t.lecturas[args.SensorID]
	if !ok {
		return fmt.Errorf("no existe lectura para el sensor %s", args.SensorID)
	}

	*resp = lectura
	return nil
}
