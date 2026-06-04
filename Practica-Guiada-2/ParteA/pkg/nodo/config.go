package nodo

import (
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Configuracion contiene los datos necesarios para identificar el nodo y conectarlo al broker.
type Configuracion struct {
	ID                string
	Edificio          string
	Aula              string
	BrokerMQTT        string
	IntervaloSegundos time.Duration
}

// CargarConfiguracion lee variables de entorno, aplica valores por defecto y valida los datos.
func CargarConfiguracion() Configuracion {
	id := obtenerEnv("NODO_ID", "nodo-01")
	edificio := obtenerEnv("NODO_EDIFICIO", "ingenieria")
	aula := obtenerEnv("NODO_AULA", "lab3")
	broker := obtenerEnv("MQTT_BROKER", "localhost:1883")
	intervalo := obtenerEnv("INTERVALO_SEGUNDOS", "5")

	validarNombre("NODO_ID", id)
	validarNombre("NODO_EDIFICIO", edificio)
	validarNombre("NODO_AULA", aula)

	if strings.TrimSpace(broker) == "" {
		log.Fatal("MQTT_BROKER no puede estar vacio")
	}

	segundos, err := strconv.Atoi(intervalo)
	if err != nil || segundos <= 0 {
		log.Fatalf("INTERVALO_SEGUNDOS debe ser un numero positivo. Valor recibido: %s", intervalo)
	}

	return Configuracion{
		ID:                id,
		Edificio:          edificio,
		Aula:              aula,
		BrokerMQTT:        broker,
		IntervaloSegundos: time.Duration(segundos) * time.Second,
	}
}

// obtenerEnv devuelve el valor de una variable de entorno o un valor por defecto.
func obtenerEnv(clave, valorPorDefecto string) string {
	if v := os.Getenv(clave); v != "" {
		return v
	}
	return valorPorDefecto
}

// validarNombre evita valores vacíos o con caracteres problemáticos para los tópicos MQTT.
func validarNombre(campo, valor string) {
	valor = strings.TrimSpace(valor)

	if valor == "" {
		log.Fatalf("%s no puede estar vacio", campo)
	}

	patron := regexp.MustCompile(`^[a-zA-Z0-9-]+$`)

	if !patron.MatchString(valor) {
		log.Fatalf("%s solo puede contener letras, numeros y guiones. Valor recibido: %s", campo, valor)
	}
}
