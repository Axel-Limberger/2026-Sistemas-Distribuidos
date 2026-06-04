package coap

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"sync"
	"time"

	"sd-iot/pkg/nodo"
	"sd-iot/pkg/sensor"

	coap "github.com/plgd-dev/go-coap/v3"
	"github.com/plgd-dev/go-coap/v3/message"
	"github.com/plgd-dev/go-coap/v3/message/codes"
	"github.com/plgd-dev/go-coap/v3/mux"
)

// ServidorCoAP expone recursos CoAP para consultar temperatura y modificar configuración local.
type ServidorCoAP struct {
	sim    *sensor.Simulador
	config nodo.Configuracion
	mu     sync.RWMutex
	modo   string
}

// NuevoServidor inicializa el servidor CoAP asociado al simulador y a la configuración del nodo.
func NuevoServidor(sim *sensor.Simulador, config nodo.Configuracion) *ServidorCoAP {
	return &ServidorCoAP{
		sim:    sim,
		config: config,
		modo:   "automatico",
	}
}

// Iniciar registra los recursos CoAP y levanta el servidor UDP en el puerto 5683.
func (s *ServidorCoAP) Iniciar() {
	router := mux.NewRouter()

	router.HandleFunc("/temperatura", s.handleTemperatura)
	router.HandleFunc("/config", s.handleConfig)

	log.Println("Servidor CoAP escuchando en UDP :5683")

	if err := coap.ListenAndServe("udp", ":5683", router); err != nil {
		log.Fatalf("Error iniciando servidor CoAP: %v", err)
	}
}

// handleTemperatura responde GET /temperatura con la última lectura del sensor.
func (s *ServidorCoAP) handleTemperatura(w mux.ResponseWriter, req *mux.Message) {
	if req.Code() != codes.GET {
		responderJSON(w, codes.MethodNotAllowed, map[string]string{
			"error": "metodo no permitido",
		})
		return
	}

	respuesta := map[string]interface{}{
		"nodo_id":     s.config.ID,
		"temperatura": s.sim.ObtenerUltima(),
		"unidad":      "C",
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}

	responderJSON(w, codes.Content, respuesta)
}

// handleConfig permite consultar la configuración con GET o actualizarla con PUT.
func (s *ServidorCoAP) handleConfig(w mux.ResponseWriter, req *mux.Message) {
	switch req.Code() {
	case codes.GET:
		s.mu.RLock()
		respuesta := map[string]interface{}{
			"nodo_id":            s.config.ID,
			"edificio":           s.config.Edificio,
			"aula":               s.config.Aula,
			"modo":               s.modo,
			"intervalo_segundos": int(s.config.IntervaloSegundos.Seconds()),
			"broker_mqtt":        s.config.BrokerMQTT,
		}
		s.mu.RUnlock()

		responderJSON(w, codes.Content, respuesta)

	case codes.PUT:
		body := req.Body()
		if body == nil {
			responderJSON(w, codes.BadRequest, map[string]string{
				"error": "body vacio",
			})
			return
		}

		data, err := io.ReadAll(body)
		if err != nil {
			responderJSON(w, codes.BadRequest, map[string]string{
				"error": "no se pudo leer el body",
			})
			return
		}

		var entrada struct {
			Modo              string `json:"modo"`
			IntervaloSegundos int    `json:"intervalo_segundos"`
		}

		if err := json.Unmarshal(data, &entrada); err != nil {
			responderJSON(w, codes.BadRequest, map[string]string{
				"error": "json invalido",
			})
			return
		}

		s.mu.Lock()

		if entrada.Modo != "" {
			s.modo = entrada.Modo
		}

		if entrada.IntervaloSegundos > 0 {
			s.config.IntervaloSegundos = time.Duration(entrada.IntervaloSegundos) * time.Second
		}

		respuesta := map[string]interface{}{
			"mensaje":            "configuracion actualizada",
			"modo":               s.modo,
			"intervalo_segundos": int(s.config.IntervaloSegundos.Seconds()),
		}

		s.mu.Unlock()

		responderJSON(w, codes.Changed, respuesta)

	default:
		responderJSON(w, codes.MethodNotAllowed, map[string]string{
			"error": "metodo no permitido",
		})
	}
}

// responderJSON serializa una respuesta a JSON y la envía con el código CoAP correspondiente.
func responderJSON(w mux.ResponseWriter, codigo codes.Code, datos interface{}) {
	payload, err := json.Marshal(datos)
	if err != nil {
		log.Printf("Error generando JSON CoAP: %v", err)
		return
	}

	if err := w.SetResponse(codigo, message.AppJSON, bytes.NewReader(payload)); err != nil {
		log.Printf("Error enviando respuesta CoAP: %v", err)
	}
}
