package mqtt

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"sd-iot/pkg/nodo"
	"sd-iot/pkg/sensor"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// Cliente administra la conexión MQTT del nodo y sus operaciones principales.
type Cliente struct {
	config   nodo.Configuracion
	interno  mqtt.Client
	opciones *mqtt.ClientOptions
}

// NuevoCliente configura el cliente MQTT con broker, ClientID, reconexión automática
// y testamento LWT para informar estado offline ante una desconexión inesperada.
func NuevoCliente(config nodo.Configuracion) (*Cliente, error) {
	topicoEstado := fmt.Sprintf("nodo/%s/estado", config.ID)

	opciones := mqtt.NewClientOptions()
	opciones.AddBroker(normalizarBroker(config.BrokerMQTT))
	opciones.SetClientID(config.ID)
	opciones.SetConnectTimeout(5 * time.Second)
	opciones.SetKeepAlive(30 * time.Second)
	opciones.SetPingTimeout(10 * time.Second)
	opciones.SetAutoReconnect(true)
	opciones.SetConnectRetry(true)
	opciones.SetConnectRetryInterval(3 * time.Second)
	opciones.SetCleanSession(true)

	opciones.SetWill(topicoEstado, `{"estado":"offline"}`, 1, true)

	cliente := mqtt.NewClient(opciones)

	return &Cliente{
		config:   config,
		interno:  cliente,
		opciones: opciones,
	}, nil
}

// Conectar inicia la sesión MQTT y publica el estado online del nodo como mensaje retenido.
func (c *Cliente) Conectar() error {
	token := c.interno.Connect()

	if !token.WaitTimeout(10 * time.Second) {
		return fmt.Errorf("timeout conectando al broker MQTT")
	}

	if err := token.Error(); err != nil {
		return err
	}

	topicoEstado := fmt.Sprintf("nodo/%s/estado", c.config.ID)

	token = c.interno.Publish(topicoEstado, 1, true, `{"estado":"online"}`)

	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("timeout publicando estado online")
	}

	if err := token.Error(); err != nil {
		return err
	}

	log.Printf("Estado publicado: %s -> online", topicoEstado)

	return nil
}

// PublicarLecturas envía periódicamente lecturas simuladas de temperatura al tópico MQTT del aula.
func (c *Cliente) PublicarLecturas(sim *sensor.Simulador, config nodo.Configuracion) {
	topico := fmt.Sprintf(
		"campus/%s/%s/sensor/temperatura",
		config.Edificio,
		config.Aula,
	)

	publicar := func() {
		temperatura := sim.Leer()

		payload := map[string]interface{}{
			"nodo_id":     config.ID,
			"temperatura": temperatura,
			"unidad":      "C",
			"timestamp":   time.Now().UTC().Format(time.RFC3339),
		}

		mensaje, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error serializando lectura: %v", err)
			return
		}

		token := c.interno.Publish(topico, 1, false, mensaje)

		if !token.WaitTimeout(5 * time.Second) {
			log.Printf("Timeout publicando lectura en %s", topico)
			return
		}

		if err := token.Error(); err != nil {
			log.Printf("Error publicando lectura: %v", err)
			return
		}

		log.Printf("Lectura publicada en %s: %s", topico, string(mensaje))
	}

	publicar()

	ticker := time.NewTicker(config.IntervaloSegundos)
	defer ticker.Stop()

	for range ticker.C {
		publicar()
	}
}

// SuscribirComandos escucha comandos MQTT del actuador y simula su ejecución por consola.
func (c *Cliente) SuscribirComandos(config nodo.Configuracion) error {
	topico := fmt.Sprintf(
		"campus/%s/%s/actuador/cmd",
		config.Edificio,
		config.Aula,
	)

	callback := func(client mqtt.Client, msg mqtt.Message) {
		var comando struct {
			Accion string `json:"accion"`
			Origen string `json:"origen"`
		}

		if err := json.Unmarshal(msg.Payload(), &comando); err != nil {
			log.Printf("Comando inválido en %s: %s", msg.Topic(), string(msg.Payload()))
			return
		}

		log.Printf(
			"Comando recibido en %s: accion=%s, origen=%s",
			msg.Topic(),
			comando.Accion,
			comando.Origen,
		)

		if comando.Accion != "" {
			log.Printf("Ejecutando acción simulada: %s", comando.Accion)
		}
	}

	token := c.interno.Subscribe(topico, 1, callback)

	if !token.WaitTimeout(5 * time.Second) {
		return fmt.Errorf("timeout suscribiendo a comandos")
	}

	if err := token.Error(); err != nil {
		return err
	}

	log.Printf("Suscripto a comandos en %s", topico)

	return nil
}

// Desconectar publica el estado offline y cierra la sesión MQTT de forma limpia.
func (c *Cliente) Desconectar() {
	if c.interno == nil || !c.interno.IsConnected() {
		return
	}

	topicoEstado := fmt.Sprintf("nodo/%s/estado", c.config.ID)

	token := c.interno.Publish(topicoEstado, 1, true, `{"estado":"offline"}`)
	token.WaitTimeout(5 * time.Second)

	if err := token.Error(); err != nil {
		log.Printf("Error publicando estado offline: %v", err)
	} else {
		log.Printf("Estado publicado: %s -> offline", topicoEstado)
	}

	c.interno.Disconnect(250)
}

// normalizarBroker agrega el protocolo tcp:// si la URL del broker no lo incluye.
func normalizarBroker(broker string) string {
	if strings.HasPrefix(broker, "tcp://") ||
		strings.HasPrefix(broker, "ssl://") ||
		strings.HasPrefix(broker, "ws://") ||
		strings.HasPrefix(broker, "wss://") {
		return broker
	}

	return "tcp://" + broker
}
